// Command payments is a miniature payouts service built on Workrail: an HTTP
// API and a worker in one process, moving money through a fake ACH rail.
//
// It exists to show what an application that embeds Workrail actually looks
// like — idempotent enqueue at the edge, checkpointed steps around side
// effects, permanent failures separated from transient ones, and compensation
// when a transfer comes back. Every failure it demonstrates is reproducible:
// the amount and routing number decide which path a payout takes.
//
//	DATABASE_URL=postgres://... go run ./examples/payments
//
// See README.md in this directory for the full walkthrough.
package main

import (
	"context"
	_ "embed"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stemitom/workrail"
)

//go:embed schema.sql
var schemaSQL string

// App is the service: its own database tables, a fake bank, and a Workrail
// client used both to enqueue jobs from HTTP handlers and to run the worker.
type App struct {
	db     *pgxpool.Pool
	ledger *Ledger
	bank   *Bank
	client *workrail.Client
	logger *slog.Logger
}

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = workrail.DefaultDatabaseURL
	}
	addr := os.Getenv("PAYMENTS_ADDR")
	if addr == "" {
		addr = ":8090"
	}

	db, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.Exec(ctx, schemaSQL); err != nil {
		return err
	}
	if err := seedAccounts(ctx, db); err != nil {
		return err
	}

	client, err := workrail.Open(ctx, workrail.Options{DatabaseURL: databaseURL})
	if err != nil {
		return err
	}
	defer client.Close()

	app := &App{db: db, ledger: &Ledger{db: db}, bank: &Bank{db: db}, client: client, logger: slog.Default()}
	client.Register(workflowPayout, app.payoutWorkflow)
	client.Register(workflowSettlement, app.settlementWorkflow)

	server := &http.Server{
		Addr:              addr,
		Handler:           app.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	go func() {
		app.logger.Info("payments api listening", "addr", addr, "dashboard", dashboardURL())
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			app.logger.Error("http server failed", "error", err)
			stop()
		}
	}()

	// The worker runs in this same process, which is the point of the embedded
	// SDK: the code that handles the HTTP request and the code that executes
	// the workflow are one deployable.
	return client.RunWorker(ctx, workrail.WorkerOptions{
		ID:              workerID(),
		Queue:           Queue,
		Concurrency:     4,
		PollInterval:    time.Second,
		LeaseDuration:   30 * time.Second,
		ShutdownTimeout: 20 * time.Second,
	})
}

func workerID() string {
	if id := os.Getenv("PAYMENTS_WORKER_ID"); id != "" {
		return id
	}
	return "payments-worker-1"
}

// dashboardURL is only used to link the demo UI at the Workrail dashboard.
func dashboardURL() string {
	if url := os.Getenv("WORKRAIL_DASHBOARD_URL"); url != "" {
		return url
	}
	return "http://localhost:8080"
}

func seedAccounts(ctx context.Context, db *pgxpool.Pool) error {
	_, err := db.Exec(ctx, `
		INSERT INTO accounts (id, name, balance_cents) VALUES
			('acct_ada', 'Ada Lovelace', 500000),
			('acct_grace', 'Grace Hopper', 25000)
		ON CONFLICT (id) DO NOTHING
	`)
	return err
}
