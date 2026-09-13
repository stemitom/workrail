# Operations

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

Schedule work for later:

```bash
go run ./cmd/workrail enqueue --queue default --type sleep --payload '{"seconds":1}' --delay 30s
```

Named queues let different worker pools own different classes of work:

```bash
go run ./cmd/workrail enqueue --queue emails --type send_email --payload '{"user_id":"user_123"}'
WORKRAIL_QUEUE=emails go run ./cmd/workrail worker
WORKRAIL_QUEUE=billing go run ./cmd/workrail worker
```

Workers only claim jobs from their configured queue. Jobs default to the `default` queue when no queue is provided.

CLI commands print compact tables for humans by default. Add `--json` to `enqueue`, `list`, `inspect`, and `dlq` commands when scripting.

## State machine

```text
queued -> running -> succeeded
   |         |   \-> retrying -> running (after backoff)
   |         \-----> dead_letter
   \---------------> canceled
```

Workers claim tasks with `FOR UPDATE SKIP LOCKED`, set a lease deadline, emit heartbeats, and complete or fail the job transactionally. Expired leases are reclaimed by later claims, which is the core failure recovery path. Workers also run a periodic sweep (every lease duration, across all queues) that moves running jobs with expired leases and exhausted attempts to `dead_letter`, so a job that repeatedly kills its worker cannot loop forever. When a worker's heartbeat is rejected — its lease was reclaimed or the job was canceled — it cancels the workflow context and stops executing that job; if heartbeats keep failing for any other reason, the worker cancels the job before its unrenewed lease expires rather than finish work it may no longer own.

Workers stop claiming new jobs when their process context is canceled. In-flight jobs are allowed to drain for `WORKRAIL_SHUTDOWN_TIMEOUT`; if that timeout elapses, Workrail cancels the in-flight workflow contexts so jobs can fail or be reclaimed by lease expiry. Workflow panics are recovered and recorded as job failures, so a single bad workflow does not crash the worker process.

## Retention

Retention is off by default. Set `worker.retention` (or `WORKRAIL_RETENTION`) to e.g. `168h` and workers will prune `succeeded` and `canceled` jobs (and their events) in their own queue older than that, in bounded batches during the periodic sweep. Invalid duration values fail at startup rather than silently defaulting. Dead-lettered jobs are never pruned automatically — they wait for an operator.
