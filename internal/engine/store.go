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
	DeadLetterExhausted(ctx context.Context) (int, error)
	PruneCompleted(ctx context.Context, queue string, olderThan time.Duration) (int, error)
	Complete(ctx context.Context, jobID, workerID string, result []byte) error
	Fail(ctx context.Context, jobID, workerID string, cause error) error
	Cancel(ctx context.Context, jobID string) error
	RetryDeadLetter(ctx context.Context, jobID string) (Job, error)
	Replay(ctx context.Context, jobID string) (Job, error)
	Get(ctx context.Context, jobID string) (Job, []Event, error)
	List(ctx context.Context, opts ListOptions) ([]Job, error)
	QueueDepth(ctx context.Context) ([]QueueDepth, error)
}
