package engine

import (
	"context"
	"encoding/json"
	"fmt"
)

type stepRunnerKey struct{}

type stepRunner struct {
	store StepStore
	jobID string
}

// StepStore is the subset of Store that RunStep needs to checkpoint results.
type StepStore interface {
	GetStep(ctx context.Context, jobID, stepName string) (json.RawMessage, bool, error)
	SaveStep(ctx context.Context, jobID, stepName string, result json.RawMessage) error
}

func WithStepRunner(ctx context.Context, store StepStore, jobID string) context.Context {
	return context.WithValue(ctx, stepRunnerKey{}, &stepRunner{store: store, jobID: jobID})
}

// RunStep executes fn at most once per job: a completed step's checkpointed
// result is returned on later attempts instead of running fn again, so a
// retried workflow resumes after its last completed step. Without a runner in
// ctx (outside a worker) fn just runs, which keeps plain tests working.
func RunStep(ctx context.Context, name string, fn func(context.Context) (json.RawMessage, error)) (json.RawMessage, error) {
	runner, _ := ctx.Value(stepRunnerKey{}).(*stepRunner)
	if runner == nil {
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
	if err := runner.store.SaveStep(ctx, runner.jobID, name, result); err != nil {
		return nil, fmt.Errorf("save step %q: %w", name, err)
	}
	return result, nil
}
