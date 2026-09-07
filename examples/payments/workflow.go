package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/stemitom/workrail"
)

// Scenario switches. A demo needs failures on demand, and a financial demo
// needs them reproducible, so the request decides which path it takes rather
// than a random number generator.
const (
	// rejectRoutingNumber is refused by the rail outright — a permanent error.
	rejectRoutingNumber = "000000000"
	// flakyCents: amounts ending in .13 fail transiently flakyFailures times
	// before clearing, which is what makes retries and step resumption visible.
	flakyCents    = 13
	flakyFailures = 2
	// returnedCents: amounts ending in .99 settle as an ACH return, so the
	// compensating path runs and the held funds go back.
	returnedCents = 99
)

// Queue is the Workrail queue this application's jobs live on. Keeping payouts
// on their own queue means a backlog of some other workload cannot starve them.
const Queue = "payouts"

const (
	workflowPayout     = "payout"
	workflowSettlement = "payout-settlement"
)

// Destination is where the money is going. account_number is exactly the kind
// of field that must never be rendered in an operator console — the demo runs
// the API with WORKRAIL_REDACT_FIELDS set so the dashboard masks it.
type Destination struct {
	AccountName   string `json:"account_name"`
	AccountNumber string `json:"account_number"`
	RoutingNumber string `json:"routing_number"`
}

// payoutJob is the workflow payload. It is deliberately a snapshot of the
// request rather than a pointer to it: a job that reads mutable rows at
// execution time can behave differently on retry than it did on attempt one.
type payoutJob struct {
	PayoutID    string      `json:"payout_id"`
	AccountID   string      `json:"account_id"`
	AmountCents int64       `json:"amount_cents"`
	Destination Destination `json:"destination"`
}

type settlementJob struct {
	PayoutID string `json:"payout_id"`
}

// transferKey is the idempotency key handed to the bank. It is derived from
// the payout id, so every retry of the submit step — and every replay of the
// job — asks the rail to move the same money once.
func transferKey(payoutID string) string { return "payout:" + payoutID }

// payoutWorkflow moves money once, in checkpointed steps.
//
// Each Step's result is persisted before the next one runs, so a crash, a lost
// lease, or a retry after a rail outage resumes at the first unfinished step
// instead of re-reserving funds or re-submitting the transfer. The guarantee
// is effectively once, not exactly once: a crash between a step's side effect
// and its checkpoint re-runs that step, which is why the ledger is keyed on
// the payout id and the rail on transferKey.
func (a *App) payoutWorkflow(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var job payoutJob
	if err := json.Unmarshal(payload, &job); err != nil {
		// A payload this workflow cannot parse will not parse on retry either.
		return nil, workrail.Permanent(err)
	}

	hold, err := workrail.Step(ctx, "reserve-funds", func(ctx context.Context) (Hold, error) {
		hold, err := a.ledger.Reserve(ctx, job.PayoutID, job.AccountID, job.AmountCents)
		if errors.Is(err, ErrInsufficientFunds) {
			return Hold{}, workrail.Permanent(err)
		}
		return hold, err
	})
	if err != nil {
		return nil, a.abandon(ctx, job.PayoutID, err)
	}

	transfer, err := workrail.Step(ctx, "submit-ach", func(ctx context.Context) (Transfer, error) {
		transfer, err := a.bank.Submit(ctx, transferKey(job.PayoutID), job.AmountCents, job.Destination)
		if errors.Is(err, ErrRejected) {
			return Transfer{}, workrail.Permanent(err)
		}
		return transfer, err
	})
	if err != nil {
		return nil, a.abandon(ctx, job.PayoutID, err)
	}

	// Workrail has no in-workflow timer, and blocking here would hold a worker
	// slot for the whole settlement window. Instead the workflow schedules its
	// own follow-up: a separate job that becomes claimable once the rail has
	// had time to settle. The idempotency key makes the enqueue safe to repeat.
	if _, err := workrail.Step(ctx, "schedule-settlement", func(ctx context.Context) (string, error) {
		settlement, _, err := a.client.EnqueueJSON(ctx, workflowSettlement, settlementJob{PayoutID: job.PayoutID},
			workrail.WithQueue(Queue),
			workrail.WithIdempotencyKey("settle:"+job.PayoutID),
			workrail.WithMaxAttempts(settlementAttempts),
			workrail.WithRunAfter(time.Now().UTC().Add(settleLag)),
		)
		if err != nil {
			return "", err
		}
		return settlement.ID, nil
	}); err != nil {
		return nil, err
	}

	if _, err := workrail.Step(ctx, "mark-submitted", func(ctx context.Context) (bool, error) {
		_, err := a.db.Exec(ctx, `
			UPDATE payouts SET status = 'submitted', bank_ref = $2, updated_at = now()
			WHERE id = $1 AND status = 'pending'
		`, job.PayoutID, transfer.Ref)
		return err == nil, err
	}); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"bank_ref":     transfer.Ref,
		"held_cents":   hold.AmountCents,
		"hold_existed": hold.Reused,
	})
}

// settlementAttempts bounds how long the settlement check keeps polling before
// the job dead-letters for an operator. With Workrail's backoff capped at a
// minute, ten attempts covers roughly eight minutes.
const settlementAttempts = 10

// settlementWorkflow finishes a payout once the rail resolves it.
//
// "Not settled yet" is returned as an ordinary error on purpose: Workrail
// re-runs the job after a backoff, which is how a workflow waits when the
// engine has no signals or durable timers. Nothing before the wait is redone,
// because this is a separate job with nothing before the wait in it.
func (a *App) settlementWorkflow(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var job settlementJob
	if err := json.Unmarshal(payload, &job); err != nil {
		return nil, workrail.Permanent(err)
	}

	transfer, err := a.bank.Status(ctx, transferKey(job.PayoutID))
	if err != nil {
		return nil, err
	}
	if transfer.Status == "pending" {
		return nil, fmt.Errorf("%w: %s", ErrNotSettled, transfer.Ref)
	}

	if transfer.Status == "returned" {
		// The bank gave the money back. Release the hold and tell the customer:
		// the workflow succeeded at handling a return, so this is not a failure.
		if _, err := workrail.Step(ctx, "release-funds", func(ctx context.Context) (bool, error) {
			return true, a.ledger.Release(ctx, job.PayoutID)
		}); err != nil {
			return nil, err
		}
		if _, err := workrail.Step(ctx, "notify-returned", func(ctx context.Context) (bool, error) {
			return true, a.notify(ctx, job.PayoutID, "email",
				fmt.Sprintf("Your payout was returned by the receiving bank (%s). The funds are back in your balance.", transfer.Ref))
		}); err != nil {
			return nil, err
		}
		if err := a.setPayoutStatus(ctx, job.PayoutID, "returned", "returned by receiving bank"); err != nil {
			return nil, err
		}
		return json.Marshal(map[string]any{"status": "returned", "bank_ref": transfer.Ref})
	}

	if _, err := workrail.Step(ctx, "post-ledger", func(ctx context.Context) (bool, error) {
		return true, a.ledger.Post(ctx, job.PayoutID)
	}); err != nil {
		return nil, err
	}
	if _, err := workrail.Step(ctx, "notify-paid", func(ctx context.Context) (bool, error) {
		return true, a.notify(ctx, job.PayoutID, "email",
			fmt.Sprintf("Your payout of %s has settled (%s).", formatCents(transfer.AmountCents), transfer.Ref))
	}); err != nil {
		return nil, err
	}
	if err := a.setPayoutStatus(ctx, job.PayoutID, "paid", ""); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"status": "paid", "bank_ref": transfer.Ref})
}

// abandon compensates a payout that cannot proceed. Only permanent failures
// are compensated here: a transient one is about to be retried, and releasing
// the hold under it would leave the retry reserving funds all over again. A
// payout whose retries are exhausted stops in dead_letter with its hold
// intact, waiting for an operator — Workrail has no hook that fires when a job
// is dead-lettered, so nothing else can safely unwind it.
func (a *App) abandon(ctx context.Context, payoutID string, cause error) error {
	if !workrail.IsPermanent(cause) {
		return cause
	}
	if err := a.ledger.Release(ctx, payoutID); err != nil {
		return errors.Join(cause, err)
	}
	if err := a.setPayoutStatus(ctx, payoutID, "failed", cause.Error()); err != nil {
		return errors.Join(cause, err)
	}
	if err := a.notify(ctx, payoutID, "email", "Your payout could not be sent: "+cause.Error()); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (a *App) setPayoutStatus(ctx context.Context, payoutID, status, reason string) error {
	_, err := a.db.Exec(ctx, `
		UPDATE payouts SET status = $2, failure_reason = NULLIF($3, ''), updated_at = now() WHERE id = $1
	`, payoutID, status, reason)
	return err
}

func (a *App) notify(ctx context.Context, payoutID, channel, body string) error {
	_, err := a.db.Exec(ctx, `
		INSERT INTO notifications (payout_id, channel, body) VALUES ($1, $2, $3)
	`, payoutID, channel, body)
	return err
}

// formatCents renders a whole-cent amount as dollars with thousands
// separators. Money in this demo never goes near a float.
func formatCents(cents int64) string {
	sign := ""
	if cents < 0 {
		sign, cents = "-", -cents
	}
	dollars := strconv.FormatInt(cents/100, 10)
	for i := len(dollars) - 3; i > 0; i -= 3 {
		dollars = dollars[:i] + "," + dollars[i:]
	}
	return fmt.Sprintf("%s$%s.%02d", sign, dollars, cents%100)
}
