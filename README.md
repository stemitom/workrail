# Workrail

A mini Temporal-style durable workflow engine in Go. It includes an API server, worker runtime, PostgreSQL-backed state, idempotent enqueue, retries with exponential backoff, dead-letter handling, heartbeats, lease-based task claiming, OpenTelemetry spans, Prometheus metrics, and a CLI.

## Quick Start

```bash
docker compose up --build
```

The API listens on `http://localhost:8080`.

```bash
go run ./cmd/dwf migrate
go run ./cmd/dwf enqueue --type echo --payload '{"message":"hello"}' --idempotency-key demo-1
go run ./cmd/dwf list
go run ./cmd/dwf inspect <job-id>
go run ./cmd/dwf cancel <job-id>
go run ./cmd/dwf replay <job-id>
```

Run the Postgres-backed integration tests against a local database:

```bash
go run ./cmd/dwf migrate
make integration-test
```

## Architecture

- `cmd/dwf`: single binary with `api`, `worker`, and CLI commands.
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

## Environment

- `DATABASE_URL`: PostgreSQL connection string. Defaults to `postgres://durable:durable@localhost:5432/durable?sslmode=disable`.
- `DWF_API_ADDR`: API listen address. Defaults to `:8080`.
- `DWF_WORKER_ID`: worker identity. Defaults to hostname.
