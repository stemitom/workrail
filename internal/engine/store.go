package engine

import "context"

type Store interface {
	Enqueue(ctx context.Context, req EnqueueRequest) (Job, bool, error)
	Claim(ctx context.Context, opts ClaimOptions) ([]Job, error)
	Heartbeat(ctx context.Context, jobID, workerID string) error
	Complete(ctx context.Context, jobID, workerID string, result []byte) error
	Fail(ctx context.Context, jobID, workerID string, cause error) error
	Cancel(ctx context.Context, jobID string) error
	RetryDeadLetter(ctx context.Context, jobID string) (Job, error)
	Replay(ctx context.Context, jobID string) (Job, error)
	Get(ctx context.Context, jobID string) (Job, []Event, error)
	List(ctx context.Context, opts ListOptions) ([]Job, error)
}
