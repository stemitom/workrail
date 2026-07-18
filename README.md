# Workrail

A mini Temporal-style durable workflow engine in Go. It includes an API server, worker runtime, PostgreSQL-backed state, idempotent enqueue, retries with exponential backoff, dead-letter handling, heartbeats, lease-based task claiming, OpenTelemetry spans, Prometheus metrics, and a CLI.

```bash
go get github.com/stemitom/workrail
```

See `examples/embedded` for registering workflows and running a worker inside your own service.

## Quick Start

```bash
docker compose up --build
```

The API listens on `http://localhost:8080`.
Worker Prometheus metrics listen on `http://localhost:9090` in Docker Compose.

```bash
go run ./cmd/workrail migrate up
go run ./cmd/workrail enqueue --queue default --type echo --payload '{"message":"hello"}' --idempotency-key demo-1
go run ./cmd/workrail list
go run ./cmd/workrail list --queue default
go run ./cmd/workrail list --status dead_letter
go run ./cmd/workrail inspect <job-id>
go run ./cmd/workrail dlq list
go run ./cmd/workrail dlq retry <job-id>
go run ./cmd/workrail cancel <job-id>
go run ./cmd/workrail replay <job-id>
```

CLI commands print compact tables for humans by default. Add `--json` to `enqueue`, `list`, `inspect`, and `dlq` commands when scripting.

Use a config file instead of environment variables:

```bash
cp workrail.example.yaml workrail.yaml
go run ./cmd/workrail --config workrail.yaml api
go run ./cmd/workrail --config workrail.yaml worker
```

Run the Postgres-backed integration tests against a local database:

```bash
go run ./cmd/workrail migrate up
make integration-test
```

## Architecture

- `cmd/workrail`: single binary with `api`, `worker`, and CLI commands.
- `workrail.go`: public Go SDK for embedding clients and workers.
- `internal/engine`: job model, state machine, workflow registry, worker runtime.
- `internal/store/postgres`: durable SQL implementation using row locks and leases.
- `internal/observability`: OpenTelemetry and Prometheus setup.
- `migrations`: versioned PostgreSQL migrations.

## State Machine

```text
queued -> running -> succeeded
   |         |   \-> retrying -> running (after backoff)
   |         \-----> dead_letter
   \---------------> canceled
```

Workers claim tasks with `FOR UPDATE SKIP LOCKED`, set a lease deadline, emit heartbeats, and complete or fail the job transactionally. Expired leases are reclaimed by later claims, which is the core failure recovery path. Workers also run a periodic sweep (every lease duration, across all queues) that moves running jobs with expired leases and exhausted attempts to `dead_letter`, so a job that repeatedly kills its worker cannot loop forever. When a worker's heartbeat is rejected — its lease was reclaimed or the job was canceled — it cancels the workflow context and stops executing that job; if heartbeats keep failing for any other reason, the worker cancels the job before its unrenewed lease expires rather than finish work it may no longer own.

Workers stop claiming new jobs when their process context is canceled. In-flight jobs are allowed to drain for `WORKRAIL_SHUTDOWN_TIMEOUT`; if that timeout elapses, Workrail cancels the in-flight workflow contexts so jobs can fail or be reclaimed by lease expiry. Workflow panics are recovered and recorded as job failures, so a single bad workflow does not crash the worker process.

## Durable Steps

Workflows can checkpoint intermediate results so retries resume after the last completed step instead of redoing work:

```go
client.Register("order", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	charge, err := workrail.Step(ctx, "charge-card", func(ctx context.Context) (ChargeResult, error) {
		return billing.Charge(ctx, order)
	})
	if err != nil {
		return nil, err
	}
	_, err = workrail.Step(ctx, "send-receipt", func(ctx context.Context) (bool, error) {
		return true, email.SendReceipt(ctx, order, charge)
	})
	return json.Marshal(charge)
})
```

If `send-receipt` fails, the retry skips `charge-card` and returns its saved result — the card is charged once. Step results persist in `job_steps`, appear as `job.step_completed` events in `workrail inspect`, survive dead-letter retries, and are deleted with their job. `replay` creates a new job, so a replayed workflow starts fresh. Step names must be stable across deploys; results must round-trip through JSON. The built-in `sequence` workflow checkpoints each of its steps automatically.

## Security

Set `api.auth_token` in the config file (or `WORKRAIL_API_TOKEN`) to require `Authorization: Bearer <token>` on every API endpoint except `GET /healthz`. With no token configured the API is open and logs a warning at startup — do not run it that way outside local development. Prometheus can scrape the protected `/metrics` endpoint with `authorization.credentials` in its scrape config.

## Retention

Retention is off by default. Set `worker.retention` (or `WORKRAIL_RETENTION`) to e.g. `168h` and workers will prune `succeeded` and `canceled` jobs (and their events) in their own queue older than that, in bounded batches during the periodic sweep. Invalid duration values fail at startup rather than silently defaulting. Dead-lettered jobs are never pruned automatically — they wait for an operator.

## Operations

List recent jobs:

```bash
go run ./cmd/workrail list --limit 50
go run ./cmd/workrail list --queue emails
go run ./cmd/workrail list --status queued
go run ./cmd/workrail list --type send_email
```

Inspect one job with its event history:

```bash
go run ./cmd/workrail inspect <job-id>
```

Operate the dead-letter queue:

```bash
go run ./cmd/workrail dlq list
go run ./cmd/workrail dlq retry <job-id>
```

Retrying a dead-lettered job moves it back to `queued`, clears the last error, and resets the attempt counter.

Named queues let different worker pools own different classes of work:

```bash
go run ./cmd/workrail enqueue --queue emails --type send_email --payload '{"user_id":"user_123"}'
WORKRAIL_QUEUE=emails go run ./cmd/workrail worker
WORKRAIL_QUEUE=billing go run ./cmd/workrail worker
```

Workers only claim jobs from their configured queue. Jobs default to the `default` queue when no queue is provided.

## Metrics

The API exposes Prometheus metrics at `/metrics`. Standalone workers expose metrics on `WORKRAIL_WORKER_METRICS_ADDR`, defaulting to `:9090`.

Key metrics include:

- `workrail_jobs_enqueued_total{queue,workflow_type}`
- `workrail_jobs_claimed_total{queue,workflow_type}`
- `workrail_jobs_succeeded_total{queue,workflow_type}`
- `workrail_jobs_failed_total{queue,workflow_type}`
- `workrail_job_heartbeats_total{queue}`
- `workrail_worker_inflight_jobs{queue,workflow_type}`
- `workrail_worker_configured_concurrency{worker_id,queue}`
- `workrail_queue_depth{queue,status}`

## Tracing

Workrail can export OpenTelemetry traces over OTLP/gRPC:

```yaml
tracing:
  enabled: true
  endpoint: localhost:4317
  insecure: true
```

Environment overrides are also available:

- `WORKRAIL_TRACING_ENABLED`: set to `true` or `1` to enable OTLP export.
- `WORKRAIL_OTLP_ENDPOINT`: OTLP/gRPC endpoint, for example `localhost:4317`. Standard `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and `OTEL_EXPORTER_OTLP_ENDPOINT` are also honored.
- `WORKRAIL_OTLP_INSECURE`: set to `true` or `1` for plaintext local collectors.

## Workflow Definitions

Built-in Go workflows live in `internal/engine/workflows.go`. JSON/YAML workflow specs can be submitted as payloads for the `sequence` workflow:

```yaml
steps:
  - name: first
    activity: echo
    input:
      message: hello
  - name: wait
    activity: sleep
    input:
      seconds: 2
```

## Go SDK

Applications can embed a worker and register workflows directly:

```go
client, err := workrail.Open(ctx, workrail.Options{
    DatabaseURL: os.Getenv("DATABASE_URL"),
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

client.Register("send_email", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
    return json.RawMessage(`{"sent":true}`), nil
})

go client.RunWorker(ctx, workrail.WorkerOptions{
    ID:          "emails-1",
    Queue:       "emails",
    Concurrency: 8,
})

job, inserted, err := client.EnqueueJSON(ctx, "send_email", map[string]any{
    "user_id": "user_123",
}, workrail.WithQueue("emails"), workrail.WithIdempotencyKey("welcome-email-user_123"))
```

## Environment

Workrail loads defaults first, then `workrail.yaml` if it exists, then environment variables. Set `WORKRAIL_CONFIG=/path/to/workrail.yaml` or pass `--config /path/to/workrail.yaml` before the command.

- `DATABASE_URL`: PostgreSQL connection string. Defaults to `postgres://durable:durable@localhost:5432/durable?sslmode=disable`.
- `WORKRAIL_API_ADDR`: API listen address. Defaults to `:8080`.
- `WORKRAIL_QUEUE`: worker queue subscription. Defaults to `default`.
- `WORKRAIL_WORKER_ID`: worker identity. Defaults to hostname.
- `WORKRAIL_WORKER_CONCURRENCY`: number of jobs a worker runs concurrently. Defaults to `4`.
- `WORKRAIL_SHUTDOWN_TIMEOUT`: graceful worker drain timeout. Defaults to `30s`.
- `WORKRAIL_WORKER_METRICS_ADDR`: worker Prometheus metrics listen address. Defaults to `:9090`; set empty to disable.

## Migrations

Run all pending migrations:

```bash
go run ./cmd/workrail migrate up
```

Workrail records applied versions in `schema_migrations`, so rerunning the command is safe. Migration files must be named like `001_init.sql`.
