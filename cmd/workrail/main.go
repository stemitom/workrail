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
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"workrail/internal/api"
	appconfig "workrail/internal/config"
	"workrail/internal/engine"
	"workrail/internal/migrations"
	"workrail/internal/observability"
	"workrail/internal/store/postgres"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	configPath, args, err := parseGlobalArgs(os.Args[1:])
	if err != nil {
		return err
	}
	if len(args) < 1 {
		usage()
		return nil
	}
	cfg, err := appconfig.Load(configPath)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch args[0] {
	case "api":
		return runAPI(ctx, cfg)
	case "worker":
		return runWorker(ctx, cfg)
	case "migrate":
		return migrate(ctx, cfg, args[1:])
	case "enqueue":
		return enqueue(ctx, cfg, args[1:])
	case "list":
		return listJobs(ctx, cfg, args[1:])
	case "dlq":
		return dlq(ctx, cfg, args[1:])
	case "inspect":
		return inspectJob(ctx, cfg, args[1:])
	case "cancel":
		return cancelJob(ctx, cfg, args[1:])
	case "replay":
		return replayJob(ctx, cfg, args[1:])
	default:
		usage()
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func parseGlobalArgs(args []string) (string, []string, error) {
	if len(args) >= 2 && (args[0] == "--config" || args[0] == "-config") {
		return args[1], args[2:], nil
	}
	if len(args) == 1 && (args[0] == "--config" || args[0] == "-config") {
		return "", nil, fmt.Errorf("--config requires a path")
	}
	return "", args, nil
}

func migrate(ctx context.Context, cfg appconfig.Config, args []string) error {
	command, dir, err := parseMigrateArgs(args)
	if err != nil {
		return err
	}
	if command != "up" {
		return fmt.Errorf("unknown migrate command %q", command)
	}
	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	applied, err := migrations.Up(ctx, db, dir)
	if err != nil {
		return err
	}
	if len(applied) == 0 {
		fmt.Fprintln(os.Stdout, "migrations already up to date")
		return nil
	}
	for _, migration := range applied {
		fmt.Fprintf(os.Stdout, "applied %s %s\n", migration.Version, migration.Name)
	}
	return nil
}

func parseMigrateArgs(args []string) (string, string, error) {
	command := "up"
	dir := "migrations"
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "up":
			command = "up"
		case "--dir":
			if i+1 >= len(args) {
				return "", "", fmt.Errorf("--dir requires a path")
			}
			dir = args[i+1]
			i++
		default:
			if strings.HasPrefix(args[i], "--dir=") {
				dir = strings.TrimPrefix(args[i], "--dir=")
				continue
			}
			return "", "", fmt.Errorf("unknown migrate argument %q", args[i])
		}
	}
	return command, dir, nil
}

func runAPI(ctx context.Context, cfg appconfig.Config) error {
	observability.Init("workrail-api")
	store, err := postgres.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer store.Close()

	addr := cfg.API.Addr
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

func runWorker(ctx context.Context, cfg appconfig.Config) error {
	observability.Init("workrail-worker")
	store, err := postgres.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	observability.RegisterQueueDepthCollector(store)
	if err := startMetricsServer(ctx, cfg.Worker.MetricsAddr); err != nil {
		return err
	}
	hostname, _ := os.Hostname()
	workerID := cfg.Worker.ID
	if workerID == "" {
		workerID = hostname
	}
	if workerID == "" {
		workerID = "worker"
	}
	w := &engine.Worker{
		ID:              workerID,
		Queue:           cfg.Worker.Queue,
		Store:           store,
		Registry:        engine.NewRegistry(),
		PollInterval:    time.Second,
		LeaseDuration:   30 * time.Second,
		ShutdownTimeout: parseDuration(cfg.Worker.ShutdownTimeout, 30*time.Second),
		Concurrency:     cfg.Worker.Concurrency,
		Logger:          slog.Default(),
	}
	return w.Run(ctx)
}

func startMetricsServer(ctx context.Context, addr string) error {
	if addr == "" {
		return nil
	}
	server := &http.Server{
		Addr:              addr,
		Handler:           promhttp.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	go func() {
		slog.Info("worker metrics listening", "addr", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("worker metrics server failed", "error", err)
		}
	}()
	return nil
}

func enqueue(ctx context.Context, cfg appconfig.Config, args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	queue := fs.String("queue", "default", "queue name")
	typ := fs.String("type", "echo", "workflow type")
	payload := fs.String("payload", "{}", "JSON or YAML payload")
	key := fs.String("idempotency-key", "", "idempotency key")
	maxAttempts := fs.Int("max-attempts", 3, "max attempts")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	req := engine.EnqueueRequest{
		Queue:          *queue,
		WorkflowType:   *typ,
		Payload:        json.RawMessage(*payload),
		IdempotencyKey: *key,
		MaxAttempts:    *maxAttempts,
	}
	store, err := postgres.New(ctx, cfg.DatabaseURL)
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
	fmt.Fprintf(os.Stdout, "%s\t%s\t%s\t%s\n", verb, job.ID, job.Queue, job.Status)
	return nil
}

func listJobs(ctx context.Context, cfg appconfig.Config, args []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	limit := fs.Int("limit", 20, "limit")
	queue := fs.String("queue", "", "filter by queue")
	status := fs.String("status", "", "filter by status")
	typ := fs.String("type", "", "filter by workflow type")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := postgres.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	jobs, err := store.List(ctx, engine.ListOptions{
		Limit:        *limit,
		Queue:        *queue,
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

func inspectJob(ctx context.Context, cfg appconfig.Config, args []string) error {
	fs := flag.NewFlagSet("inspect", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: workrail inspect <job-id>")
	}
	store, err := postgres.New(ctx, cfg.DatabaseURL)
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

func cancelJob(ctx context.Context, cfg appconfig.Config, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: workrail cancel <job-id>")
	}
	store, err := postgres.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	return store.Cancel(ctx, args[0])
}

func replayJob(ctx context.Context, cfg appconfig.Config, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: workrail replay <job-id>")
	}
	store, err := postgres.New(ctx, cfg.DatabaseURL)
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

func dlq(ctx context.Context, cfg appconfig.Config, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: workrail dlq <list|retry>")
	}
	switch args[0] {
	case "list":
		return listDeadLetters(ctx, cfg, args[1:])
	case "retry":
		return retryDeadLetter(ctx, cfg, args[1:])
	default:
		return fmt.Errorf("unknown dlq command %q", args[0])
	}
}

func listDeadLetters(ctx context.Context, cfg appconfig.Config, args []string) error {
	fs := flag.NewFlagSet("dlq list", flag.ExitOnError)
	limit := fs.Int("limit", 20, "limit")
	queue := fs.String("queue", "", "filter by queue")
	typ := fs.String("type", "", "filter by workflow type")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := postgres.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	jobs, err := store.List(ctx, engine.ListOptions{
		Limit:        *limit,
		Queue:        *queue,
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

func retryDeadLetter(ctx context.Context, cfg appconfig.Config, args []string) error {
	fs := flag.NewFlagSet("dlq retry", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: workrail dlq retry <job-id>")
	}
	store, err := postgres.New(ctx, cfg.DatabaseURL)
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
	fmt.Fprintln(w, "ID\tQUEUE\tTYPE\tSTATUS\tATTEMPT\tMAX\tRUN_AFTER\tUPDATED")
	for _, job := range jobs {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%d\t%s\t%s\n",
			shortID(job.ID),
			job.Queue,
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
	fmt.Fprintf(os.Stdout, "queue: %s\n", job.Queue)
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

func parseDuration(value string, fallback time.Duration) time.Duration {
	duration, err := time.ParseDuration(value)
	if err != nil {
		slog.Warn("invalid duration config value", "value", value, "error", err)
		return fallback
	}
	return duration
}

func usage() {
	fmt.Fprintln(os.Stderr, `workrail commands:
  --config workrail.yaml <command>
  api
  worker
  migrate up [--dir migrations]
  enqueue --queue default --type echo --payload '{"message":"hi"}' [--idempotency-key key]
  list [--limit 20] [--queue default] [--status queued] [--type echo] [--json]
  inspect [--json] <job-id>
  replay <job-id>
  cancel <job-id>
  dlq list [--limit 20] [--queue default] [--type echo] [--json]
  dlq retry [--json] <job-id>`)
}
