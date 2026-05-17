# Workrail

A mini Temporal-style durable workflow engine in Go. It includes an API server, worker runtime, PostgreSQL-backed state, idempotent enqueue, retries with exponential backoff, dead-letter handling, heartbeats, lease-based task claiming, OpenTelemetry spans, Prometheus metrics, and a CLI.

## Quick Start

```bash
docker compose up --build
```

The API listens on `http://localhost:8080`.

```bash
go run ./cmd/workrail migrate
go run ./cmd/workrail enqueue --type echo --payload '{"message":"hello"}' --idempotency-key demo-1
go run ./cmd/workrail list
go run ./cmd/workrail list --status dead_letter
go run ./cmd/workrail inspect <job-id>
go run ./cmd/workrail dlq list
go run ./cmd/workrail dlq retry <job-id>
go run ./cmd/workrail cancel <job-id>
go run ./cmd/workrail replay <job-id>
```

CLI commands print compact tables for humans by default. Add `--json` to `enqueue`, `list`, `inspect`, and `dlq` commands when scripting.

Run the Postgres-backed integration tests against a local database:

```bash
go run ./cmd/workrail migrate
make integration-test
```

## Architecture

- `cmd/workrail`: single binary with `api`, `worker`, and CLI commands.
- `workrail.go`: public Go SDK for embedding clients and workers.
- `internal/engine`: job model, state machine, workflow registry, worker runtime.
- `internal/store/postgres`: durable SQL implementation using row locks and leases.
- `internal/observability`: OpenTelemetry and Prometheus setup.
- `migrations`: PostgreSQL schema.

## State Machine

```text
queued -> running -> succeeded
   |         |   \-> retrying -> queued
   |         \----> failed -> dead_letter
   \--------------> canceled
```

Workers claim tasks with `FOR UPDATE SKIP LOCKED`, set a lease deadline, emit heartbeats, and complete or fail the job transactionally. Expired leases are reclaimed by later claims, which is the core failure recovery path.

Workers stop claiming new jobs when their process context is canceled. In-flight jobs are allowed to drain for `WORKRAIL_SHUTDOWN_TIMEOUT`; if that timeout elapses, Workrail cancels the in-flight workflow contexts so jobs can fail or be reclaimed by lease expiry. Workflow panics are recovered and recorded as job failures, so a single bad workflow does not crash the worker process.

## Operations

List recent jobs:

```bash
go run ./cmd/workrail list --limit 50
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
    Concurrency: 8,
})

job, inserted, err := client.EnqueueJSON(ctx, "send_email", map[string]any{
    "user_id": "user_123",
}, workrail.WithIdempotencyKey("welcome-email-user_123"))
```

## Environment

- `DATABASE_URL`: PostgreSQL connection string. Defaults to `postgres://durable:durable@localhost:5432/durable?sslmode=disable`.
- `WORKRAIL_API_ADDR`: API listen address. Defaults to `:8080`.
- `WORKRAIL_WORKER_ID`: worker identity. Defaults to hostname.
- `WORKRAIL_WORKER_CONCURRENCY`: number of jobs a worker runs concurrently. Defaults to `4`.
- `WORKRAIL_SHUTDOWN_TIMEOUT`: graceful worker drain timeout. Defaults to `30s`.
