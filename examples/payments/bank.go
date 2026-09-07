package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrRailUnavailable is a fault: the same request may well succeed on the
	// next attempt, so the workflow leaves it unwrapped and lets Workrail back
	// off and retry.
	ErrRailUnavailable = errors.New("ach rail unavailable")
	// ErrRejected is the bank refusing the transfer itself. Retrying re-sends
	// a request the bank has already judged invalid, so the workflow marks it
	// permanent.
	ErrRejected = errors.New("transfer rejected")
	// ErrNotSettled means the transfer is still in flight. The settlement
	// workflow returns it so Workrail re-runs the check after a backoff — the
	// closest thing to a durable timer this engine has today.
	ErrNotSettled = errors.New("transfer has not settled yet")
)

// settleLag is how long a submitted transfer stays pending before the fake
// rail decides its fate. Short enough to watch happen in the dashboard.
const settleLag = 6 * time.Second

// Bank is a fake ACH rail backed by a table. It behaves like a real one in the
// ways that matter to a workflow engine: submissions are idempotent on a
// caller-supplied key, some attempts fail transiently, some are rejected
// outright, and settlement happens later than submission.
type Bank struct {
	db *pgxpool.Pool
}

// Transfer is checkpointed by the workflow, so it round-trips through JSON.
type Transfer struct {
	Ref         string `json:"ref"`
	Status      string `json:"status"`
	AmountCents int64  `json:"amount_cents"`
	Attempts    int    `json:"attempts"`
}

// Submit sends a transfer, or returns the existing one for this idempotency
// key. Which scenario a request hits is decided by the request itself, so the
// demo is reproducible — see the table in README.md.
func (b *Bank) Submit(ctx context.Context, key string, amountCents int64, dest Destination) (Transfer, error) {
	// The rail remembers keys it has already accepted. This check is what
	// makes a re-run of the submit step safe: it returns the first transfer
	// instead of moving money again.
	if transfer, found, err := b.lookup(ctx, key); err != nil || found {
		return transfer, err
	}

	// Counted before the outcome is decided, and on its own statement, so a
	// failed submission still burns an attempt.
	attempts, err := b.recordAttempt(ctx, key)
	if err != nil {
		return Transfer{}, err
	}
	if dest.RoutingNumber == rejectRoutingNumber {
		return Transfer{}, fmt.Errorf("%w: invalid routing number %s", ErrRejected, dest.RoutingNumber)
	}
	if amountCents%100 == flakyCents && attempts <= flakyFailures {
		return Transfer{}, fmt.Errorf("%w (attempt %d of %d before it clears)", ErrRailUnavailable, attempts, flakyFailures+1)
	}

	// Short enough to read in a table, unique enough for a demo.
	ref := fmt.Sprintf("ach_%08x", time.Now().UnixNano()%(1<<32))
	if _, err := b.db.Exec(ctx, `
		INSERT INTO bank_transfers (idempotency_key, ref, amount_cents, attempts, settle_after)
		VALUES ($1, $2, $3, $4, now() + $5::interval)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, key, ref, amountCents, attempts, settleLag.String()); err != nil {
		return Transfer{}, err
	}
	transfer, found, err := b.lookup(ctx, key)
	if err != nil {
		return Transfer{}, err
	}
	if !found {
		return Transfer{}, fmt.Errorf("transfer %q vanished after insert", key)
	}
	return transfer, nil
}

// Status resolves a pending transfer once its settlement lag has elapsed.
func (b *Bank) Status(ctx context.Context, key string) (Transfer, error) {
	var transfer Transfer
	err := pgx.BeginFunc(ctx, b.db, func(tx pgx.Tx) error {
		var due bool
		if err := tx.QueryRow(ctx, `
			SELECT ref, status, amount_cents, attempts, settle_after <= now()
			FROM bank_transfers WHERE idempotency_key = $1 FOR UPDATE
		`, key).Scan(&transfer.Ref, &transfer.Status, &transfer.AmountCents, &transfer.Attempts, &due); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("unknown transfer %q", key)
			}
			return err
		}
		if transfer.Status != "pending" || !due {
			return nil
		}
		transfer.Status = "settled"
		if transfer.AmountCents%100 == returnedCents {
			transfer.Status = "returned"
		}
		_, err := tx.Exec(ctx, `UPDATE bank_transfers SET status = $2 WHERE idempotency_key = $1`, key, transfer.Status)
		return err
	})
	if err != nil {
		return Transfer{}, err
	}
	return transfer, nil
}

func (b *Bank) lookup(ctx context.Context, key string) (Transfer, bool, error) {
	var transfer Transfer
	err := b.db.QueryRow(ctx, `
		SELECT ref, status, amount_cents, attempts FROM bank_transfers WHERE idempotency_key = $1
	`, key).Scan(&transfer.Ref, &transfer.Status, &transfer.AmountCents, &transfer.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return Transfer{}, false, nil
	}
	if err != nil {
		return Transfer{}, false, err
	}
	return transfer, true, nil
}

// recordAttempt counts submissions per key, including the ones that fail,
// which is what lets an injected transient failure stop after a fixed number
// of tries instead of failing forever.
func (b *Bank) recordAttempt(ctx context.Context, key string) (int, error) {
	var attempts int
	err := b.db.QueryRow(ctx, `
		INSERT INTO bank_attempts (idempotency_key, attempts) VALUES ($1, 1)
		ON CONFLICT (idempotency_key) DO UPDATE SET attempts = bank_attempts.attempts + 1
		RETURNING attempts
	`, key).Scan(&attempts)
	return attempts, err
}
