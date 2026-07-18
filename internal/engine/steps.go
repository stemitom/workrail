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
}

// StepStore is the subset of Store that RunStep needs to checkpoint results.
// SaveStep returns the persisted result: on a first-write-wins race the
// winner's checkpoint comes back, not the caller's input.
type StepStore interface {
	GetStep(ctx context.Context, jobID, stepName string) (json.RawMessage, bool, error)
	SaveStep(ctx context.Context, jobID, workerID, stepName string, result json.RawMessage) (json.RawMessage, error)
}

func WithStepRunner(ctx context.Context, store StepStore, jobID, workerID string) context.Context {
	return context.WithValue(ctx, stepRunnerKey{}, stepRunner{store: store, jobID: jobID, workerID: workerID})
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
	cached, found, err := runner.store.GetStep(ctx, runner.jobID, name)
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
	stored, err := runner.store.SaveStep(saveCtx, runner.jobID, runner.workerID, name, result)
	if err != nil {
		return nil, fmt.Errorf("save step %q: %w", name, err)
	}
	return stored, nil
}
