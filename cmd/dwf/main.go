package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"workrail/internal/api"
	"workrail/internal/engine"
	"workrail/internal/observability"
	"workrail/internal/store/postgres"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultDatabaseURL = "postgres://durable:durable@localhost:5432/durable?sslmode=disable"

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		usage()
		return nil
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch os.Args[1] {
	case "api":
		return runAPI(ctx)
	case "worker":
		return runWorker(ctx)
	case "migrate":
		return migrate(ctx, os.Args[2:])
	case "enqueue":
		return enqueue(ctx, os.Args[2:])
	case "list":
		return listJobs(ctx, os.Args[2:])
	case "inspect":
		return inspectJob(ctx, os.Args[2:])
	case "cancel":
		return cancelJob(ctx, os.Args[2:])
	case "replay":
		return replayJob(ctx, os.Args[2:])
	default:
		usage()
		return fmt.Errorf("unknown command %q", os.Args[1])
	}
}

func migrate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
	file := fs.String("file", "migrations/001_init.sql", "migration file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sql, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	db, err := pgxpool.New(ctx, databaseURL())
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.Exec(ctx, string(sql)); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "applied %s\n", *file)
	return nil
}

func runAPI(ctx context.Context) error {
	observability.Init("workrail-api")
	store, err := postgres.New(ctx, databaseURL())
	if err != nil {
		return err
	}
	defer store.Close()

	addr := env("DWF_API_ADDR", ":8080")
	server := &http.Server{
		Addr:              addr,
		Handler:           api.New(store, slog.Default()).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	slog.Info("api listening", "addr", addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func runWorker(ctx context.Context) error {
	observability.Init("workrail-worker")
	store, err := postgres.New(ctx, databaseURL())
	if err != nil {
		return err
	}
	defer store.Close()
	hostname, _ := os.Hostname()
	workerID := env("DWF_WORKER_ID", hostname)
	if workerID == "" {
		workerID = "worker"
	}
	w := &engine.Worker{
		ID:            workerID,
		Store:         store,
		Registry:      engine.NewRegistry(),
		PollInterval:  time.Second,
		LeaseDuration: 30 * time.Second,
		Concurrency:   4,
		Logger:        slog.Default(),
	}
	return w.Run(ctx)
}

func enqueue(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	typ := fs.String("type", "echo", "workflow type")
	payload := fs.String("payload", "{}", "JSON or YAML payload")
	key := fs.String("idempotency-key", "", "idempotency key")
	maxAttempts := fs.Int("max-attempts", 3, "max attempts")
	if err := fs.Parse(args); err != nil {
		return err
	}
	req := engine.EnqueueRequest{
		WorkflowType:   *typ,
		Payload:        json.RawMessage(*payload),
		IdempotencyKey: *key,
		MaxAttempts:    *maxAttempts,
	}
	store, err := postgres.New(ctx, databaseURL())
	if err != nil {
		return err
	}
	defer store.Close()
	job, _, err := store.Enqueue(ctx, req)
	if err != nil {
		return err
	}
	return printJSON(job)
}

func listJobs(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	limit := fs.Int("limit", 20, "limit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := postgres.New(ctx, databaseURL())
	if err != nil {
		return err
	}
	defer store.Close()
	jobs, err := store.List(ctx, *limit)
	if err != nil {
		return err
	}
	return printJSON(jobs)
}

func inspectJob(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: dwf inspect <job-id>")
	}
	store, err := postgres.New(ctx, databaseURL())
	if err != nil {
		return err
	}
	defer store.Close()
	job, events, err := store.Get(ctx, args[0])
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"job": job, "events": events})
}

func cancelJob(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: dwf cancel <job-id>")
	}
	store, err := postgres.New(ctx, databaseURL())
	if err != nil {
		return err
	}
	defer store.Close()
	return store.Cancel(ctx, args[0])
}

func replayJob(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: dwf replay <job-id>")
	}
	store, err := postgres.New(ctx, databaseURL())
	if err != nil {
		return err
	}
	defer store.Close()
	job, err := store.Replay(ctx, args[0])
	if err != nil {
		return err
	}
	return printJSON(job)
}

func printJSON(value any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func databaseURL() string {
	return env("DATABASE_URL", defaultDatabaseURL)
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func usage() {
	fmt.Fprintln(os.Stderr, `dwf commands:
  api
  worker
  migrate [--file migrations/001_init.sql]
  enqueue --type echo --payload '{"message":"hi"}' [--idempotency-key key]
  list [--limit 20]
  inspect <job-id>
  replay <job-id>
  cancel <job-id>`)
}
