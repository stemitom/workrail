# Configuration

Workrail loads defaults first, then `workrail.yaml` if it exists, then environment variables. Set `WORKRAIL_CONFIG=/path/to/workrail.yaml` or pass `--config /path/to/workrail.yaml` before the command:

```bash
cp workrail.example.yaml workrail.yaml
go run ./cmd/workrail --config workrail.yaml api
go run ./cmd/workrail --config workrail.yaml worker
```

- `DATABASE_URL`: PostgreSQL connection string. Defaults to `postgres://durable:durable@localhost:5432/durable?sslmode=disable`.
- `WORKRAIL_API_ADDR`: API listen address. Defaults to `:8080`.
- `WORKRAIL_QUEUE`: worker queue subscription. Defaults to `default`.
- `WORKRAIL_WORKER_ID`: worker identity. Defaults to hostname.
- `WORKRAIL_WORKER_CONCURRENCY`: number of jobs a worker runs concurrently. Defaults to `4`.
- `WORKRAIL_SHUTDOWN_TIMEOUT`: graceful worker drain timeout. Defaults to `30s`.
- `WORKRAIL_WORKER_METRICS_ADDR`: worker Prometheus metrics listen address. Defaults to `:9090`; set empty to disable.
- `WORKRAIL_API_TOKEN`: bearer token required by the API, and the dashboard sign-in secret. Empty disables auth.
- `WORKRAIL_REDACT_FIELDS`: comma-separated JSON field names masked in everything the dashboard renders.
- `WORKRAIL_RETENTION`: prune succeeded and canceled jobs older than this. Defaults to off.

## Security

Set `api.auth_token` in the config file (or `WORKRAIL_API_TOKEN`) to require `Authorization: Bearer <token>` on every API endpoint except `GET /healthz`. With no token configured the API is open and logs a warning at startup — do not run it that way outside local development. Prometheus can scrape the protected `/metrics` endpoint with `authorization.credentials` in its scrape config.

### Redaction

Workflow payloads carry whatever the application put in them, and the dashboard renders payloads, results, step checkpoints, and event details verbatim to anyone holding a session. Set `dashboard.redact_fields` (or `WORKRAIL_REDACT_FIELDS=account_number,ssn`) to mask those fields wherever the dashboard prints JSON. Matching is case-insensitive and ignores `-` and `_`, so one entry covers `account_number`, `accountNumber`, and `Account-Number`, and it applies at every depth including inside arrays.

Redaction deliberately does not extend to the JSON API: that surface authenticates with the bearer token, which a dashboard session cannot use, and its machine callers need the real payload. Nor does it protect data at rest — the payload is still stored unencrypted in `jobs.payload`. Treat it as keeping sensitive values off an operator's screen, not as a substitute for keeping them out of payloads.

## Migrations

Run all pending migrations:

```bash
go run ./cmd/workrail migrate up
```

Workrail records applied versions in `schema_migrations`, so rerunning the command is safe. Migration files must be named like `001_init.sql`.

Run the Postgres-backed integration tests against a migrated database:

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
