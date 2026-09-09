package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type registryKey struct{}

// WithRegistry makes the workflow registry available to ExecuteActivity. The
// worker installs it on every job context; without it activities cannot run.
func WithRegistry(ctx context.Context, reg *Registry) context.Context {
	return context.WithValue(ctx, registryKey{}, reg)
}

// pushFrame returns ctx with the checkpoint scope extended by name. Activity
// and signal/timer names must not contain "/"; it separates scope levels.
func pushFrame(ctx context.Context, name string) context.Context {
	runner, ok := ctx.Value(stepRunnerKey{}).(stepRunner)
	if !ok {
		return ctx
	}
	frame := name
	if runner.frame != "" {
		frame = runner.frame + "/" + name
	}
	return context.WithValue(ctx, stepRunnerKey{}, stepRunner{
		store: runner.store, jobID: runner.jobID, workerID: runner.workerID, frame: frame,
	})
}

// ExecuteActivity runs a registered activity with its own checkpoint scope.
// Steps, timers, and signal waits inside the activity are namespaced under
// the activity path, so two activities using the same step name checkpoint
// independently instead of colliding. Scopes nest: an activity calling
// ExecuteActivity extends the path. Workflows must call side effects through
// ExecuteActivity (or a Step function) rather than inline, so retries replay
// checkpoints instead of repeating the world.
func ExecuteActivity(ctx context.Context, name string, input json.RawMessage) (json.RawMessage, error) {
	if _, ok := ctx.Value(stepRunnerKey{}).(stepRunner); !ok {
		return nil, errors.New("workrail: ExecuteActivity requires a worker")
	}
	reg, _ := ctx.Value(registryKey{}).(*Registry)
	if reg == nil {
		return nil, errors.New("workrail: ExecuteActivity requires a registered workflow runtime")
	}
	activity, ok := reg.activities[name]
	if !ok {
		return nil, fmt.Errorf("unknown activity %q", name)
	}
	return activity(pushFrame(ctx, name), input)
}
