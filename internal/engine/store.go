package engine

import (
	"context"
	"time"
)

type Store interface {
	StepStore
	Enqueue(ctx context.Context, req EnqueueRequest) (Job, bool, error)
	Claim(ctx context.Context, opts ClaimOptions) ([]Job, error)
	Heartbeat(ctx context.Context, jobID, workerID string, leaseDuration time.Duration) error
	ListSteps(ctx context.Context, jobID string) ([]StepResult, error)
	DeadLetterExhausted(ctx context.Context) (int, error)
	PruneCompleted(ctx context.Context, queue string, olderThan time.Duration) (int, error)
	Complete(ctx context.Context, jobID, workerID string, result []byte) error
	Fail(ctx context.Context, jobID, workerID string, cause error) error
	// Suspend re-queues a running job for runAfter without consuming a retry
	// attempt, freeing its worker slot while a durable timer waits. It parks
	// only when wakeVersion still matches the claim: a signal wake-up (or
	// cancel, or lease steal) in between makes it report false instead, and
	// the newer state wins rather than being buried under a stale nap.
	Suspend(ctx context.Context, jobID, workerID string, runAfter time.Time, wakeVersion int) (parked bool, err error)
	// Signal delivers payload to jobID's mailbox under name. Unknown job IDs
	// report ErrNotFound; jobs in a terminal state (succeeded, dead_letter,
	// canceled) report ErrInvalidTransition. A repeated idempotencyKey is a
	// Noop success even if the job has since finished: the first send wins,
	// like Temporal's RequestId dedupe.
	Signal(ctx context.Context, jobID, name string, payload []byte, idempotencyKey string) error
	Cancel(ctx context.Context, jobID string) error
	RetryDeadLetter(ctx context.Context, jobID string) (Job, error)
	Replay(ctx context.Context, jobID string) (Job, error)
	Get(ctx context.Context, jobID string) (Job, []Event, error)
	List(ctx context.Context, opts ListOptions) ([]Job, error)
	QueueDepth(ctx context.Context) ([]QueueDepth, error)
}
