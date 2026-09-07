package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrInsufficientFunds is a business rejection, not a fault: the same request
// will be refused the same way on every attempt. The workflow marks it
// permanent so Workrail dead-letters the payout instead of retrying it.
var ErrInsufficientFunds = errors.New("insufficient funds")

type Ledger struct {
	db *pgxpool.Pool
}

// Hold is the checkpointed result of the reserve step. It has to survive a
// JSON round-trip, because that is how Workrail stores it.
type Hold struct {
	PayoutID    string `json:"payout_id"`
	AccountID   string `json:"account_id"`
	AmountCents int64  `json:"amount_cents"`
	// Reused reports that the hold already existed when this attempt ran —
	// the retry-safety of the step, made visible.
	Reused bool `json:"reused"`
}

// Reserve moves money out of the available balance and records a hold. It is
// keyed on the payout id: calling it twice for the same payout debits once.
func (l *Ledger) Reserve(ctx context.Context, payoutID, accountID string, amountCents int64) (Hold, error) {
	hold := Hold{PayoutID: payoutID, AccountID: accountID, AmountCents: amountCents}
	err := pgx.BeginFunc(ctx, l.db, func(tx pgx.Tx) error {
		var existing bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM ledger_entries WHERE payout_id = $1 AND kind = 'hold')
		`, payoutID).Scan(&existing); err != nil {
			return err
		}
		if existing {
			hold.Reused = true
			return nil
		}
		var balance int64
		if err := tx.QueryRow(ctx, `
			SELECT balance_cents FROM accounts WHERE id = $1 FOR UPDATE
		`, accountID).Scan(&balance); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("unknown account %q", accountID)
			}
			return err
		}
		if balance < amountCents {
			return fmt.Errorf("%w: balance %d, requested %d", ErrInsufficientFunds, balance, amountCents)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO ledger_entries (account_id, payout_id, kind, amount_cents)
			VALUES ($1, $2, 'hold', $3)
		`, accountID, payoutID, amountCents); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			UPDATE accounts SET balance_cents = balance_cents - $2 WHERE id = $1
		`, accountID, amountCents)
		return err
	})
	if err != nil {
		return Hold{}, err
	}
	return hold, nil
}

// Post converts a hold into a settled debit. The balance already moved at
// reserve time, so this only records the entry — and only once. It copies the
// account and amount from the hold rather than trusting its caller, so a
// replayed settlement cannot post against a different account.
func (l *Ledger) Post(ctx context.Context, payoutID string) error {
	_, err := l.db.Exec(ctx, `
		INSERT INTO ledger_entries (account_id, payout_id, kind, amount_cents)
		SELECT account_id, payout_id, 'post', amount_cents
		FROM ledger_entries WHERE payout_id = $1 AND kind = 'hold'
		ON CONFLICT (payout_id, kind) DO NOTHING
	`, payoutID)
	return err
}

// Release returns held money to the available balance, once, and only if a
// hold is actually outstanding.
func (l *Ledger) Release(ctx context.Context, payoutID string) error {
	return pgx.BeginFunc(ctx, l.db, func(tx pgx.Tx) error {
		var accountID string
		var amount int64
		err := tx.QueryRow(ctx, `
			SELECT account_id, amount_cents FROM ledger_entries
			WHERE payout_id = $1 AND kind = 'hold'
		`, payoutID).Scan(&accountID, &amount)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `
			INSERT INTO ledger_entries (account_id, payout_id, kind, amount_cents)
			VALUES ($1, $2, 'release', $3)
			ON CONFLICT (payout_id, kind) DO NOTHING
		`, accountID, payoutID, amount)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		_, err = tx.Exec(ctx, `
			UPDATE accounts SET balance_cents = balance_cents + $2 WHERE id = $1
		`, accountID, amount)
		return err
	})
}
