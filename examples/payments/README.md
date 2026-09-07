# Payments — a mini financial app on Workrail

A payouts service in ~600 lines: an HTTP API, a ledger, a fake ACH rail, and
two Workrail workflows. It is the example to read if you are evaluating
Workrail for money movement, because it is built around the failures that
actually happen — a rail that times out, a bank that rejects a transfer, a
customer who double-clicks Send, a worker that dies mid-payout — rather than
around the happy path.

Every failure it demonstrates is reproducible. The amount and routing number
decide which path a payout takes, so you can run the same scenario twice and
get the same story.

## Run it

Three processes: Postgres, the Workrail API (which serves the dashboard), and
this app (which serves the payments UI and runs the worker).

```bash
export DATABASE_URL='postgres://durable:durable@localhost:5432/durable?sslmode=disable'

go run ./cmd/workrail migrate up

# Dashboard on :8080. The redact list keeps destination account numbers out of
# the operator console — see "What this exercises" below.
WORKRAIL_REDACT_FIELDS='account_number,routing_number' go run ./cmd/workrail api

# Payments UI on :8090, worker on the "payouts" queue, in one process.
go run ./examples/payments
```

Open <http://localhost:8090> and send a payout. Each row links to its job in
the Workrail dashboard, where you can watch the steps checkpoint one by one.

The app creates its own tables and seeds two accounts on startup.

## Scenarios

| Send this | What happens | What it shows |
|---|---|---|
| any amount | Reserved, submitted, settles ~6s later, ledger posted, customer notified | The ordinary path, in five checkpointed steps |
| amount ending in `.13` | The rail fails twice, then clears | Retries with backoff — and `reserve-funds` is **not** re-run, so the money is held once |
| amount ending in `.99` | Settles as an ACH return | The compensating path: the hold is released and the balance restored |
| routing number `000000000` | Rejected immediately, job dead-letters on attempt 1 of 5 | Permanent failures skip their remaining attempts |
| amount above the balance | Refused before anything leaves | A business rejection, also permanent |

Watch the flaky case in the dashboard. Its event history is the whole argument
for durable steps:

```
job.claimed          {"attempt": 1}
job.step_completed   {"step": "reserve-funds"}
job.failed           {"error": "step \"submit-ach\": ach rail unavailable (attempt 1 …)", "next_status": "retrying"}
job.claimed          {"attempt": 2}
job.failed           {"error": "step \"submit-ach\": ach rail unavailable (attempt 2 …)", "next_status": "retrying"}
job.claimed          {"attempt": 3}
job.step_completed   {"step": "submit-ach"}
job.step_completed   {"step": "schedule-settlement"}
job.step_completed   {"step": "mark-submitted"}
job.succeeded
```

Three attempts, one hold. Without checkpointing, attempts 2 and 3 would each
reserve the funds again.

## What this exercises

**Idempotent enqueue at the edge.** `POST /payouts` enqueues with
`WithIdempotencyKey("payout:" + payoutID)`. A retried request, a double-clicked
form, or a client that resends on a flaky network all converge on one job.

**Checkpointed steps around every side effect.** `reserve-funds`,
`submit-ach`, `schedule-settlement`, `mark-submitted`. A retry resumes at the
first unfinished step.

**Idempotency all the way down.** Workrail's guarantee is effectively once,
not exactly once: a crash between a step's side effect and its checkpoint
re-runs that step. So the ledger is keyed on the payout id and the rail on
`transferKey(payoutID)`. Steps whose side effects must not repeat carry their
own idempotency key — the engine reduces the retry surface, it does not remove
the requirement.

**Permanent vs transient failure.** `ErrRailUnavailable` is left unwrapped and
retried with backoff. `ErrRejected` and `ErrInsufficientFunds` are wrapped in
`workrail.Permanent`, which dead-letters the job on its first attempt instead
of re-sending a request the bank has already refused.

**Waiting without a timer.** Workrail has no signals and no in-workflow
timers, and blocking in `sleep` would hold a worker slot for the whole
settlement window. The payout workflow instead schedules its own follow-up job
with `WithRunAfter(settleLag)`, and that job returns `ErrNotSettled` while the
transfer is still in flight so the engine re-runs it after a backoff. It is a
workaround, and it is the honest state of the engine today.

**Redaction.** The destination account number lives in the job payload, and
the dashboard renders payloads verbatim to anyone holding a session. Running
the API with `WORKRAIL_REDACT_FIELDS='account_number,routing_number'` masks
them there. The JSON API is unaffected — it authenticates with the bearer
token, which the dashboard session cannot use.

## What it does not solve

Read these before assuming the pattern generalizes:

- **No dead-letter hook.** A payout that exhausts its retries stops in
  `dead_letter` with its hold still outstanding, waiting for an operator to
  retry or cancel it. `abandon()` only compensates permanent failures, because
  releasing a hold under a job that is about to retry would be worse than
  leaving it.
- **No signals.** A payout needing manual approval, or waiting on a bank
  webhook, has no way to park until the event arrives. Model it as a second
  job, as the settlement check does.
- **Sensitive data in payloads.** Redaction masks the dashboard; the payload
  is still stored unencrypted in `jobs.payload`. A production system would put
  a reference in the payload and keep the value in its own encrypted store.
- **One shared auth token.** The dashboard records that a job was retried, not
  who retried it.
