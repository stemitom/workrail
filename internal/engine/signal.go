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
// worker slot and costs no retry attempt, so this only sets worst-case
// latency for a wake-up the fast-forward missed.
const signalPollInterval = 5 * time.Second

// signalKey namespaces signal checkpoints away from Step and timer names. The
// index makes successive waits address successive deliveries: a resumed run
// replays the same indexes and lands on the same checkpoints.
func signalKey(name string, index int) string { return fmt.Sprintf("__signal:%s#%d", name, index) }

// WaitSignal blocks until the first signal with name arrives, without holding
// a worker slot. It is WaitSignalAt with index 0.
func WaitSignal(ctx context.Context, name string) (json.RawMessage, error) {
	return WaitSignalAt(ctx, name, 0)
}

// WaitSignalAt blocks until the index-th signal with name arrives (0-based,
// in delivery order), without holding a worker slot. Each delivery is
// checkpointed under its own index, so resumed runs and retries all see the
// same sequence — keep names stable and the wait order deterministic, like
// Steps. A wait past the last delivery suspends until more signals arrive.
// Do not call WaitSignalAt inside a Step function: the suspension must abort
// the workflow run, not fail the step. Outside a worker there is no mailbox
// to check, so it returns an error instead of blocking forever.
func WaitSignalAt(ctx context.Context, name string, index int) (json.RawMessage, error) {
	if index < 0 {
		return nil, fmt.Errorf("signal index %d out of range", index)
	}
	runner, ok := ctx.Value(stepRunnerKey{}).(stepRunner)
	if !ok {
		return nil, errors.New("workrail: WaitSignal requires a worker")
	}
	key := signalKey(name, index)
	if cached, found, err := runner.store.GetStep(ctx, runner.jobID, runner.scope(key)); err != nil {
		return nil, fmt.Errorf("load signal %q: %w", name, err)
	} else if found {
		return cached, nil
	}
	payload, found, err := runner.store.GetSignalAt(ctx, runner.jobID, name, index)
	if err != nil {
		return nil, fmt.Errorf("poll signal %q: %w", name, err)
	}
	if !found {
		return nil, &SuspendError{RunAfter: time.Now().UTC().Add(signalPollInterval)}
	}
	// Checkpoint the delivery first-write-wins, so duplicate executions agree
	// on which payload won the race.
	return RunStep(ctx, key, func(context.Context) (json.RawMessage, error) {
		return payload, nil
	})
}
