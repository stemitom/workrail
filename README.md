# Workrail

[![CI](https://github.com/stemitom/workrail/actions/workflows/ci.yml/badge.svg)](https://github.com/stemitom/workrail/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/stemitom/workrail.svg)](https://pkg.go.dev/github.com/stemitom/workrail)
[![Go Version](https://img.shields.io/github/go-mod/go-version/stemitom/workrail)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A mini Temporal-style durable workflow engine in Go, backed by PostgreSQL: a single binary with an API server, worker runtime, and embedded dashboard. Workflows checkpoint steps, wait on timers/signals/children without holding worker slots, retry with backoff, and unwind with compensation when they dead-letter.

```go
charge, err := workrail.Step(ctx, "charge-card", func(ctx context.Context) (ChargeResult, error) {
	return billing.Charge(ctx, order)
})
```

## Contents

- [Get started](#get-started)
- [When to use Workrail](#when-to-use-workrail)
- [Features](#features)
- [Go SDK](#go-sdk)
- [Docs](#docs)
- [Contributing](#contributing)
- [License](#license)

## Get started

```bash
docker compose up --build
go run ./cmd/workrail migrate up
go run ./cmd/workrail enqueue --queue default --type echo --payload '{"message":"hello"}' --idempotency-key demo-1
go run ./cmd/workrail list
```

The API listens on `http://localhost:8080` (dashboard at `/ui`), worker Prometheus metrics on `http://localhost:9090` in Docker Compose. See `examples/embedded` for embedding, and `examples/payments` for a complete payouts service with reproducible failure scenarios.

## When to use Workrail

- Background jobs that must survive restarts, with retries, deadlines, and an audit trail.
- Multi-step workflows where steps checkpoint, waits suspend without holding workers, and failures unwind with compensation.
- Postgres-backed simplicity: one binary plus the database you already run.

Prefer a plain queue for high-throughput fire-and-forget work. Prefer Temporal for polyglot workers, global scale, or a hosted cloud.

## Features

- **Durable steps** — retries resume after the last checkpoint instead of redoing work ([details](docs/workflows.md#durable-steps)).
- **Transient and permanent failures** — backoff retries, with `workrail.Permanent` for rejections that will never clear ([details](docs/workflows.md#failures)).
- **Waits** — `Sleep`, `WaitSignal`/`WaitSignalAt`, and `ExecuteChild` park jobs without holding worker slots ([details](docs/workflows.md#waiting)).
- **Compensation** — unwind hooks run once per dead-lettered job ([details](docs/workflows.md#compensation)).
- **Operations** — named queues, delayed enqueue, dead-letter queue, retention, human-first CLI ([details](docs/operations.md)).
- **Observability** — embedded dashboard, OpenTelemetry traces, Prometheus metrics ([details](docs/observability.md)).

## Go SDK

```bash
go get github.com/stemitom/workrail
```

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
```

`examples/payments` puts the SDK, signals, children, and compensation together in a working service.

## Docs

- [Workflows](docs/workflows.md) — steps, activities, retries, waits, children, compensation.
- [Operations](docs/operations.md) — CLI, queues, dead letters, retention.
- [Observability](docs/observability.md) — dashboard, metrics, tracing.
- [Configuration](docs/configuration.md) — config file, environment, security, migrations.

## Contributing

Issues and pull requests are welcome. Run `gofmt` and the full suite (`go test ./...` plus `make integration-test` against a local Postgres) before pushing, and keep changes minimal — the shortest working diff wins.

## License

MIT. See [LICENSE](LICENSE).
