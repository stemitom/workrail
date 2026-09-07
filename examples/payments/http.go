package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/stemitom/workrail"
)

//go:embed page.gohtml
var pageHTML string

var page = template.Must(template.New("page").Funcs(template.FuncMap{
	"money": formatCents,
	"last4": last4,
}).Parse(pageHTML))

func (a *App) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", a.index)
	mux.HandleFunc("POST /payouts", a.createPayout)
	mux.HandleFunc("GET /payouts", a.listPayouts)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

type createPayoutRequest struct {
	AccountID   string      `json:"account_id"`
	AmountCents int64       `json:"amount_cents"`
	Destination Destination `json:"destination"`
}

// createPayout writes the payout row and enqueues its workflow. The two happen
// in that order and are tied together by an idempotency key derived from the
// payout id, so a caller that retries this request — or a client that
// double-submits the form — ends up with one job, not two.
func (a *App) createPayout(w http.ResponseWriter, r *http.Request) {
	req, wantsHTML, err := decodeCreate(r)
	if err != nil {
		a.fail(w, r, wantsHTML, http.StatusBadRequest, err)
		return
	}

	ctx := r.Context()
	var payoutID string
	destination, err := json.Marshal(req.Destination)
	if err != nil {
		a.fail(w, r, wantsHTML, http.StatusBadRequest, err)
		return
	}
	if err := a.db.QueryRow(ctx, `
		INSERT INTO payouts (account_id, amount_cents, destination) VALUES ($1, $2, $3) RETURNING id
	`, req.AccountID, req.AmountCents, destination).Scan(&payoutID); err != nil {
		a.fail(w, r, wantsHTML, http.StatusBadRequest, err)
		return
	}

	job, inserted, err := a.client.EnqueueJSON(ctx, workflowPayout, payoutJob{
		PayoutID:    payoutID,
		AccountID:   req.AccountID,
		AmountCents: req.AmountCents,
		Destination: req.Destination,
	},
		workrail.WithQueue(Queue),
		// One job per payout, whatever the caller does with its retries.
		workrail.WithIdempotencyKey("payout:"+payoutID),
		workrail.WithMaxAttempts(5),
	)
	if err != nil {
		a.fail(w, r, wantsHTML, http.StatusInternalServerError, err)
		return
	}
	if _, err := a.db.Exec(ctx, `UPDATE payouts SET job_id = $2 WHERE id = $1`, payoutID, job.ID); err != nil {
		a.fail(w, r, wantsHTML, http.StatusInternalServerError, err)
		return
	}
	a.logger.Info("payout enqueued", "payout_id", payoutID, "job_id", job.ID, "new_job", inserted)

	if wantsHTML {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"payout_id": payoutID, "job_id": job.ID})
}

func decodeCreate(r *http.Request) (createPayoutRequest, bool, error) {
	var req createPayoutRequest
	if strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		err := json.NewDecoder(r.Body).Decode(&req)
		return req, false, err
	}
	if err := r.ParseForm(); err != nil {
		return req, true, err
	}
	amount, err := parseAmount(r.PostForm.Get("amount"))
	if err != nil {
		return req, true, err
	}
	req = createPayoutRequest{
		AccountID:   r.PostForm.Get("account_id"),
		AmountCents: amount,
		Destination: Destination{
			AccountName:   r.PostForm.Get("account_name"),
			AccountNumber: r.PostForm.Get("account_number"),
			RoutingNumber: r.PostForm.Get("routing_number"),
		},
	}
	return req, true, nil
}

// parseAmount reads dollars as typed by a human ("25", "25.13") into cents
// without going through float64, where 25.13 is not 2513.
func parseAmount(value string) (int64, error) {
	value = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(value), "$"))
	if value == "" {
		return 0, errors.New("amount is required")
	}
	whole, frac, _ := strings.Cut(value, ".")
	dollars, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid amount %q", value)
	}
	frac = (frac + "00")[:2]
	cents, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid amount %q", value)
	}
	total := dollars*100 + cents
	if total <= 0 {
		return 0, errors.New("amount must be positive")
	}
	return total, nil
}

type payoutView struct {
	ID            string
	AccountID     string
	AmountCents   int64
	Status        string
	BankRef       string
	FailureReason string
	JobID         string
	AccountNumber string
}

type accountView struct {
	ID           string
	Name         string
	BalanceCents int64
}

type notificationView struct {
	PayoutID string
	Channel  string
	Body     string
}

func (a *App) index(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	accounts, err := a.accounts(ctx)
	if err != nil {
		a.fail(w, r, true, http.StatusInternalServerError, err)
		return
	}
	payouts, err := a.payouts(ctx)
	if err != nil {
		a.fail(w, r, true, http.StatusInternalServerError, err)
		return
	}
	notifications, err := a.notifications(ctx)
	if err != nil {
		a.fail(w, r, true, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := page.Execute(w, map[string]any{
		"Accounts":      accounts,
		"Payouts":       payouts,
		"Notifications": notifications,
		"Dashboard":     dashboardURL(),
		"Error":         r.URL.Query().Get("error"),
	}); err != nil {
		a.logger.Error("render page", "error", err)
	}
}

func (a *App) listPayouts(w http.ResponseWriter, r *http.Request) {
	payouts, err := a.payouts(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, payouts)
}

func (a *App) accounts(ctx context.Context) ([]accountView, error) {
	rows, err := a.db.Query(ctx, `SELECT id, name, balance_cents FROM accounts ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (accountView, error) {
		var account accountView
		return account, row.Scan(&account.ID, &account.Name, &account.BalanceCents)
	})
}

func (a *App) payouts(ctx context.Context) ([]payoutView, error) {
	rows, err := a.db.Query(ctx, `
		SELECT id, account_id, amount_cents, status, COALESCE(bank_ref, ''), COALESCE(failure_reason, ''),
			COALESCE(job_id::text, ''), COALESCE(destination ->> 'account_number', '')
		FROM payouts ORDER BY created_at DESC LIMIT 25
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (payoutView, error) {
		var p payoutView
		return p, row.Scan(&p.ID, &p.AccountID, &p.AmountCents, &p.Status, &p.BankRef, &p.FailureReason, &p.JobID, &p.AccountNumber)
	})
}

func (a *App) notifications(ctx context.Context) ([]notificationView, error) {
	rows, err := a.db.Query(ctx, `
		SELECT payout_id, channel, body FROM notifications ORDER BY created_at DESC LIMIT 10
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (notificationView, error) {
		var n notificationView
		return n, row.Scan(&n.PayoutID, &n.Channel, &n.Body)
	})
}

func (a *App) fail(w http.ResponseWriter, r *http.Request, wantsHTML bool, status int, err error) {
	a.logger.Warn("request failed", "path", r.URL.Path, "error", err)
	if wantsHTML {
		http.Redirect(w, r, "/?error="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// last4 is how the application itself shows a destination account: the digits
// an operator needs to identify a payment, and none of the ones they don't.
func last4(number string) string {
	if len(number) <= 4 {
		return number
	}
	return "••••" + number[len(number)-4:]
}
