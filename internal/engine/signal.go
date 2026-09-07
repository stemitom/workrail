package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// signalPollInterval bounds how long a signal wait naps between mailbox
// checks. Signal delivery is a re-queue, not a push: each nap frees the
// worker slot and costs no retry attempt, so this only sets latency.
const signalPollInterval = 15 * time.Second

// signalKey namespaces signal checkpoints away from Step and timer names.
func signalKey(name string) string { return "__signal:" + name }

// WaitSignal blocks until a signal with name arrives, without holding a
// worker slot. The first delivered payload is checkpointed, so resumed runs
// and retries all see the same value — keep names unique per wait, like
// Steps. Do not call WaitSignal inside a Step function: the suspension must
// abort the workflow run, not fail the step. Outside a worker there is no
// mailbox to check, so it returns an error instead of blocking forever.
func WaitSignal(ctx context.Context, name string) (json.RawMessage, error) {
	runner, ok := ctx.Value(stepRunnerKey{}).(stepRunner)
	if !ok {
		return nil, errors.New("workrail: WaitSignal requires a worker")
	}
	if cached, found, err := runner.store.GetStep(ctx, runner.jobID, signalKey(name)); err != nil {
		return nil, fmt.Errorf("load signal %q: %w", name, err)
	} else if found {
		return cached, nil
	}
	payload, found, err := runner.store.GetSignal(ctx, runner.jobID, name)
	if err != nil {
		return nil, fmt.Errorf("poll signal %q: %w", name, err)
	}
	if !found {
		return nil, &SuspendError{RunAfter: time.Now().UTC().Add(signalPollInterval)}
	}
	// Checkpoint the delivery first-write-wins, so duplicate executions agree
	// on which payload won the race.
	return RunStep(ctx, signalKey(name), func(context.Context) (json.RawMessage, error) {
		return payload, nil
	})
}
