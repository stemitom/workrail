# Workflows

Register orchestration with `client.Register` and side effects with `client.RegisterActivity`. Workflows must reach the world through `Step`, `Sleep`, `WaitSignal`, or `workrail.ExecuteActivity` — never inline — so retries replay checkpoints instead of repeating side effects. Each activity runs in its own checkpoint scope, so same-named steps in different activities stay independent. Enqueueing an activity type runs it as a single-step job.

Built-in activities (`echo`, `sleep`) and the `sequence` workflow live in `internal/engine`. JSON/YAML workflow specs can be submitted as payloads for the `sequence` workflow:

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

## Durable steps

Workflows checkpoint intermediate results so retries resume after the last completed step instead of redoing work:

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

If `send-receipt` fails, the retry skips `charge-card` and returns its saved result instead of charging again. The guarantee is effectively once, not exactly once: a crash in the window between a step's side effect and its checkpoint commit re-runs the step on the next attempt, so steps whose side effects must never repeat should be idempotent (e.g. pass an idempotency key to the payment provider). A worker that loses its lease is rejected on its next checkpoint write, stopping zombie execution at the next step boundary.

Step results persist in `job_steps`, appear as `job.step_completed` events in `workrail inspect`, survive dead-letter retries (`dlq retry` resumes; use `replay` for a genuinely fresh run), and are deleted with their job. Keep step names, and result types, stable while jobs are in flight — a renamed step re-runs, and a checkpoint that no longer decodes into the step's type fails the job. Results are stored as normalized `jsonb`; don't rely on byte-identical output. The built-in `sequence` workflow checkpoints each of its steps automatically and rejects duplicate step names.

## Failures

A failed attempt is retried with exponential backoff until `max_attempts` is spent, then dead-lettered. Some failures should not be retried at all — a closed account, a rejected transfer, a payload that will never parse. Wrap those in `workrail.Permanent`:

```go
charge, err := workrail.Step(ctx, "charge-card", func(ctx context.Context) (Charge, error) {
	charge, err := billing.Charge(ctx, order)
	if errors.Is(err, billing.ErrCardDeclined) {
		return Charge{}, workrail.Permanent(err)
	}
	return charge, err // transient: let backoff and retries handle it
})
```

A permanent failure dead-letters the job on the spot with its remaining attempts unspent, and records `"permanent": true` on the `job.failed` event so the skipped retries are visible rather than mysterious. The wrapped error keeps its message and still unwraps to its cause, so `errors.Is` on the original sentinel keeps working. `workrail.IsPermanent` reports whether an error was marked. Dead-lettered jobs are never pruned and never retried automatically — `dlq retry` resumes one from its last checkpoint once the underlying problem is fixed.

## Waiting

`workrail.Sleep` parks a job until a deadline, `workrail.WaitSignal` parks it until the world responds, and `workrail.ExecuteChild` parks a parent until its child succeeds — all without holding a worker slot, and none consume retry attempts. Keep wait names stable and unique per workflow, like Steps.

```go
approval, err := workrail.WaitSignal[Approval](ctx, "approval")
if err != nil {
	return nil, err
}
```

`workrail.WaitSignalAt` consumes the index-th delivery under a name, so a workflow can read a stream of signals in order. Deliver from Go, HTTP, or the CLI:

```go
err := client.Signal(ctx, jobID, "approval", Approval{By: "ops", OK: true})
```

```bash
curl -X POST localhost:8080/jobs/<job-id>/signals \
  -H "Authorization: Bearer $WORKRAIL_API_TOKEN" \
  -d '{"name":"approval","payload":{"ok":true}}'
go run ./cmd/workrail signal <job-id> --name approval --payload '{"ok":true}'
```

Pass an idempotency key (`idempotency_key` in the API, `--idempotency-key` on the CLI, `workrail.WithSignalIdempotencyKey` in Go) and retried sends dedupe to a Noop instead of appending a second row — safe to repeat webhooks. A signal wakes a parked waiter at once: delivery fast-forwards the job's `run_after` in the same transaction, so the next worker poll (about a second by default) reclaims it. If anything is missed the wait naps 5 seconds between mailbox checks. Signaling a finished job fails, as does signaling an unknown one.

A workflow can call a whole other workflow and wait for its result:

```go
result, err := workrail.ExecuteChild[Settlement](ctx, "settle", "settlement", settlementJob{PayoutID: job.PayoutID})
if err != nil {
	return nil, err
}
```

The child is a real job with its own attempt budget and checkpoints, running on the parent's queue. The child identity is checkpointed, so resumed runs and retries watch the same child instead of spawning another. A dead-lettered or canceled child fails the parent permanently — retrying would only re-watch a finished child.

Enqueue options: `WithQueue`, `WithIdempotencyKey`, `WithMaxAttempts`, and `WithRunAfter` (the job stays unclaimable until that time — a delayed retry, or a check scheduled for later).

## Compensation

Register an unwind hook per workflow type and it runs once when a job of that type dead-letters — whether attempts ran out or the lease-expiry sweep reaped a crash-looping job:

```go
client.RegisterCompensation("payout", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	return nil, ledger.Release(ctx, payoutID)
})
```

Compensations run outside any lease, so they cannot use `Step`, `Sleep`, or `WaitSignal`: keep them to plain idempotent activities. Outcomes land in the event history as `job.compensated` / `job.compensation_failed`. Types without a hook dead-letter silently, as before. There is no cancel propagation to children: each job stands on its own.
