package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type stepRunnerKey struct{}

type stepRunner struct {
	store    StepStore
	jobID    string
	workerID string
	// queue is the parent job's queue: children enqueue there so the same
	// pool that runs the parent can run them. Empty means "default".
	queue string
	// frame namespaces checkpoints to the enclosing activity path set by
	// ExecuteActivity. Empty at the workflow top level, where names keep
	// their historical meaning so in-flight checkpoints still match.
	frame string
}

type StepResult struct {
	JobID     string          `json:"job_id"`
	Name      string          `json:"name"`
	Result    json.RawMessage `json:"result"`
	CreatedAt time.Time       `json:"created_at"`
}

// StepStore is the subset of Store that the workflow runtime needs:
// checkpointing, the signal mailbox, child job management, and job reads.
// SaveStep returns the persisted result: on a first-write-wins race the
// winner's checkpoint comes back, not the caller's input.
type StepStore interface {
	GetStep(ctx context.Context, jobID, stepName string) (json.RawMessage, bool, error)
	SaveStep(ctx context.Context, jobID, workerID, stepName string, result json.RawMessage) (json.RawMessage, error)
	// GetSignalAt returns the index-th signal delivered to jobID under name
	// (0-based, in delivery order).
	GetSignalAt(ctx context.Context, jobID, name string, index int) (json.RawMessage, bool, error)
	Enqueue(ctx context.Context, req EnqueueRequest) (Job, bool, error)
	GetJob(ctx context.Context, jobID string) (Job, error)
}

func WithStepRunner(ctx context.Context, store StepStore, jobID, workerID, queue string) context.Context {
	return context.WithValue(ctx, stepRunnerKey{}, stepRunner{store: store, jobID: jobID, workerID: workerID, queue: queue})
}

// scope namespaces name under the activity frame. The top-level frame is
// empty and returns the name unchanged.
func (r stepRunner) scope(name string) string {
	if r.frame == "" {
		return name
	}
	return r.frame + "/" + name
}

// RunStep checkpoints fn's result per job: on later attempts a completed
// step's saved result is returned instead of running fn again, so a retried
// workflow resumes after its last completed step. The guarantee is effectively
// once, not exactly once: if the process dies after fn's side effect but
// before the checkpoint commits, the step runs again on retry — make steps
// idempotent when their side effects must not repeat. Without a runner in ctx
// (outside a worker) fn just runs, which keeps plain tests working.
func RunStep(ctx context.Context, name string, fn func(context.Context) (json.RawMessage, error)) (json.RawMessage, error) {
	runner, ok := ctx.Value(stepRunnerKey{}).(stepRunner)
	if !ok {
		return fn(ctx)
	}
	cached, found, err := runner.store.GetStep(ctx, runner.jobID, runner.scope(name))
	if err != nil {
		return nil, fmt.Errorf("load step %q: %w", name, err)
	}
	if found {
		return cached, nil
	}
	result, err := fn(ctx)
	if err != nil {
		return nil, fmt.Errorf("step %q: %w", name, err)
	}
	if len(result) == 0 {
		result = json.RawMessage(`{}`)
	}
	// Persist on a non-cancelable context so a shutdown or lease-loss cancel
	// arriving after fn's side effect cannot lose the checkpoint.
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	stored, err := runner.store.SaveStep(saveCtx, runner.jobID, runner.workerID, runner.scope(name), result)
	if err != nil {
		return nil, fmt.Errorf("save step %q: %w", name, err)
	}
	return stored, nil
}
