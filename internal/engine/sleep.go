package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// SuspendError aborts the workflow run so the job can wait without holding a
// worker slot. The worker re-queues the job for RunAfter; on reclaim the
// workflow re-runs from the start and its Sleep calls return nil past their
// deadlines, so Steps before the timer are replayed from checkpoints.
type SuspendError struct {
	RunAfter time.Time
}

func (e *SuspendError) Error() string {
	return fmt.Sprintf("suspend until %s", e.RunAfter.UTC().Format(time.RFC3339))
}

// timerKey namespaces timer checkpoints away from Step names.
func timerKey(name string) string { return "__timer:" + name }

// Sleep waits d without holding a worker slot. The wake time is checkpointed
// via RunStep, so the first call suspends the job and a resumed run returns
// nil once the deadline has passed; the deadline never moves. Keep names
// stable and unique per workflow, like Steps. Do not call Sleep inside a Step
// function: the suspension must abort the workflow run, not fail the step.
// Outside a worker it just blocks.
func Sleep(ctx context.Context, name string, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	if _, ok := ctx.Value(stepRunnerKey{}).(stepRunner); !ok {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	raw, err := RunStep(ctx, timerKey(name), func(context.Context) (json.RawMessage, error) {
		return json.Marshal(time.Now().UTC().Add(d))
	})
	if err != nil {
		return err
	}
	var wake time.Time
	if err := json.Unmarshal(raw, &wake); err != nil {
		return fmt.Errorf("timer %q: decode checkpoint: %w", name, err)
	}
	if time.Now().UTC().Before(wake) {
		return &SuspendError{RunAfter: wake}
	}
	return nil
}
