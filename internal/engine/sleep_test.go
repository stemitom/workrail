package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestSleepSuspendsThenResumes(t *testing.T) {
	store := &workerTestStore{}
	ctx := WithStepRunner(context.Background(), store, "job-1", "worker-a")

	err := Sleep(ctx, "settle", time.Hour)
	var suspend *SuspendError
	if !errors.As(err, &suspend) {
		t.Fatalf("first Sleep = %v, want suspend", err)
	}
	if time.Until(suspend.RunAfter) < 50*time.Minute {
		t.Fatalf("runAfter = %v, want ~1h out", suspend.RunAfter)
	}

	// Second call before the deadline must re-suspend for the same wake time,
	// not restart the timer.
	err = Sleep(ctx, "settle", time.Hour)
	var again *SuspendError
	if !errors.As(err, &again) {
		t.Fatalf("second Sleep = %v, want suspend", err)
	}
	if !again.RunAfter.Equal(suspend.RunAfter) {
		t.Fatalf("wake moved %v -> %v, want stable deadline", suspend.RunAfter, again.RunAfter)
	}

	// A resumed run past the deadline proceeds without suspending.
	past, _ := json.Marshal(time.Now().UTC().Add(-time.Minute))
	store.steps["job-1/__timer:elapsed"] = past
	if err := Sleep(ctx, "elapsed", time.Hour); err != nil {
		t.Fatalf("elapsed Sleep = %v, want nil", err)
	}
}

func TestSleepWithoutRunnerBlocks(t *testing.T) {
	start := time.Now()
	if err := Sleep(context.Background(), "x", 20*time.Millisecond); err != nil {
		t.Fatalf("Sleep = %v, want nil", err)
	}
	if time.Since(start) < 20*time.Millisecond {
		t.Fatal("Sleep returned before the duration")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Sleep(ctx, "x", time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Sleep = %v, want context.Canceled", err)
	}
}
