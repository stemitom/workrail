package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"text/tabwriter"
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
	case "dlq":
		return dlq(ctx, os.Args[2:])
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

	addr := envAny([]string{"WORKRAIL_API_ADDR", "DWF_API_ADDR"}, ":8080")
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
	workerID := envAny([]string{"WORKRAIL_WORKER_ID", "DWF_WORKER_ID"}, hostname)
	if workerID == "" {
		workerID = "worker"
	}
	w := &engine.Worker{
		ID:              workerID,
		Store:           store,
		Registry:        engine.NewRegistry(),
		PollInterval:    time.Second,
		LeaseDuration:   30 * time.Second,
		ShutdownTimeout: envDuration("WORKRAIL_SHUTDOWN_TIMEOUT", 30*time.Second),
		Concurrency:     envInt("WORKRAIL_WORKER_CONCURRENCY", 4),
		Logger:          slog.Default(),
	}
	return w.Run(ctx)
}

func enqueue(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	typ := fs.String("type", "echo", "workflow type")
	payload := fs.String("payload", "{}", "JSON or YAML payload")
	key := fs.String("idempotency-key", "", "idempotency key")
	maxAttempts := fs.Int("max-attempts", 3, "max attempts")
	jsonOut := fs.Bool("json", false, "print JSON")
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
	job, inserted, err := store.Enqueue(ctx, req)
	if err != nil {
		return err
	}
	if *jsonOut {
		return printJSON(job)
	}
	verb := "enqueued"
	if !inserted {
		verb = "exists"
	}
	fmt.Fprintf(os.Stdout, "%s\t%s\t%s\n", verb, job.ID, job.Status)
	return nil
}

func listJobs(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	limit := fs.Int("limit", 20, "limit")
	status := fs.String("status", "", "filter by status")
	typ := fs.String("type", "", "filter by workflow type")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := postgres.New(ctx, databaseURL())
	if err != nil {
		return err
	}
	defer store.Close()
	jobs, err := store.List(ctx, engine.ListOptions{
		Limit:        *limit,
		Status:       engine.Status(*status),
		WorkflowType: *typ,
	})
	if err != nil {
		return err
	}
	if *jsonOut {
		return printJSON(jobs)
	}
	return printJobsTable(jobs)
}

func inspectJob(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("inspect", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: workrail inspect <job-id>")
	}
	store, err := postgres.New(ctx, databaseURL())
	if err != nil {
		return err
	}
	defer store.Close()
	job, events, err := store.Get(ctx, fs.Arg(0))
	if err != nil {
		return err
	}
	if !*jsonOut {
		return printJobDetails(job, events)
	}
	return printJSON(map[string]any{"job": job, "events": events})
}

func cancelJob(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: workrail cancel <job-id>")
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
		return fmt.Errorf("usage: workrail replay <job-id>")
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

func dlq(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: workrail dlq <list|retry>")
	}
	switch args[0] {
	case "list":
		return listDeadLetters(ctx, args[1:])
	case "retry":
		return retryDeadLetter(ctx, args[1:])
	default:
		return fmt.Errorf("unknown dlq command %q", args[0])
	}
}

func listDeadLetters(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("dlq list", flag.ExitOnError)
	limit := fs.Int("limit", 20, "limit")
	typ := fs.String("type", "", "filter by workflow type")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := postgres.New(ctx, databaseURL())
	if err != nil {
		return err
	}
	defer store.Close()
	jobs, err := store.List(ctx, engine.ListOptions{
		Limit:        *limit,
		Status:       engine.StatusDeadLetter,
		WorkflowType: *typ,
	})
	if err != nil {
		return err
	}
	if *jsonOut {
		return printJSON(jobs)
	}
	return printJobsTable(jobs)
}

func retryDeadLetter(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("dlq retry", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: workrail dlq retry <job-id>")
	}
	store, err := postgres.New(ctx, databaseURL())
	if err != nil {
		return err
	}
	defer store.Close()
	job, err := store.RetryDeadLetter(ctx, fs.Arg(0))
	if err != nil {
		return err
	}
	if *jsonOut {
		return printJSON(job)
	}
	fmt.Fprintf(os.Stdout, "retried\t%s\t%s\n", job.ID, job.Status)
	return nil
}

func printJSON(value any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func printJobsTable(jobs []engine.Job) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tTYPE\tSTATUS\tATTEMPT\tMAX\tRUN_AFTER\tUPDATED")
	for _, job := range jobs {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d\t%s\t%s\n",
			shortID(job.ID),
			job.WorkflowType,
			job.Status,
			job.Attempt,
			job.MaxAttempts,
			formatTime(job.RunAfter),
			formatTime(job.UpdatedAt),
		)
	}
	return w.Flush()
}

func printJobDetails(job engine.Job, events []engine.Event) error {
	fmt.Fprintf(os.Stdout, "id: %s\n", job.ID)
	fmt.Fprintf(os.Stdout, "type: %s\n", job.WorkflowType)
	fmt.Fprintf(os.Stdout, "status: %s\n", job.Status)
	fmt.Fprintf(os.Stdout, "attempt: %d/%d\n", job.Attempt, job.MaxAttempts)
	fmt.Fprintf(os.Stdout, "run_after: %s\n", formatTime(job.RunAfter))
	if job.Error != nil {
		fmt.Fprintf(os.Stdout, "error: %s\n", *job.Error)
	}
	if job.LeaseOwner != nil {
		fmt.Fprintf(os.Stdout, "lease_owner: %s\n", *job.LeaseOwner)
	}
	if len(job.Payload) > 0 {
		fmt.Fprintf(os.Stdout, "payload: %s\n", compactJSON(job.Payload))
	}
	if len(job.Result) > 0 {
		fmt.Fprintf(os.Stdout, "result: %s\n", compactJSON(job.Result))
	}
	if len(events) == 0 {
		return nil
	}
	fmt.Fprintln(os.Stdout, "\nevents:")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tTYPE\tCREATED\tDETAILS")
	for _, event := range events {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\n", event.ID, event.EventType, formatTime(event.CreatedAt), compactJSON(event.Details))
	}
	return w.Flush()
}

func shortID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

func compactJSON(data []byte) string {
	var buf bytes.Buffer
	if err := json.Compact(&buf, data); err != nil {
		return string(data)
	}
	return buf.String()
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

func envAny(keys []string, fallback string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		slog.Warn("invalid duration environment value", "key", key, "value", value, "error", err)
		return fallback
	}
	return duration
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		slog.Warn("invalid integer environment value", "key", key, "value", value, "error", err)
		return fallback
	}
	return n
}

func usage() {
	fmt.Fprintln(os.Stderr, `workrail commands:
  api
  worker
  migrate [--file migrations/001_init.sql]
  enqueue --type echo --payload '{"message":"hi"}' [--idempotency-key key]
  list [--limit 20] [--status queued] [--type echo] [--json]
  inspect [--json] <job-id>
  replay <job-id>
  cancel <job-id>
  dlq list [--limit 20] [--type echo] [--json]
  dlq retry [--json] <job-id>`)
}
