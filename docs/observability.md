# Observability

## Dashboard

The embedded web dashboard at `http://localhost:8080/ui` needs no separate process and no JavaScript build. It leads with runs needing attention, shows queue depths, a filterable and paginated job list, and a per-job view with a chronological execution path, checkpointed steps, the signal mailbox (with a send-signal form), parent/children lineage, payload/result, event history, and retry/cancel/replay/signal actions. The overview and job list update in place every few seconds without reloading. It follows the system light/dark preference. When an auth token is configured the dashboard signs in with it at `/ui/login` (session cookie; the JSON API keeps using bearer tokens).

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
