package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestRunStepWithoutRunnerJustRuns(t *testing.T) {
	calls := 0
	for range 2 {
		result, err := RunStep(context.Background(), "a", func(context.Context) (json.RawMessage, error) {
			calls++
			return json.RawMessage(`{"n":1}`), nil
		})
		if err != nil {
			t.Fatalf("run step: %v", err)
		}
		if string(result) != `{"n":1}` {
			t.Fatalf("result = %s", result)
		}
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2 (no checkpointing without a runner)", calls)
	}
}

func TestRunStepCheckpointsPerJob(t *testing.T) {
	store := &workerTestStore{}
	ctx := WithStepRunner(context.Background(), store, "job-1", "worker-a")

	calls := 0
	step := func(context.Context) (json.RawMessage, error) {
		calls++
		return json.RawMessage(`{"charged":true}`), nil
	}

	first, err := RunStep(ctx, "charge", step)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	second, err := RunStep(ctx, "charge", step)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (second run must hit the checkpoint)", calls)
	}
	if string(first) != string(second) {
		t.Fatalf("checkpointed result %s != original %s", second, first)
	}

	otherJob := WithStepRunner(context.Background(), store, "job-2", "worker-a")
	if _, err := RunStep(otherJob, "charge", step); err != nil {
		t.Fatalf("other job: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2 (checkpoints are per job)", calls)
	}
}

func TestRunStepDoesNotCheckpointFailures(t *testing.T) {
	store := &workerTestStore{}
	ctx := WithStepRunner(context.Background(), store, "job-1", "worker-a")

	calls := 0
	step := func(context.Context) (json.RawMessage, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("boom")
		}
		return json.RawMessage(`{}`), nil
	}

	if _, err := RunStep(ctx, "flaky", step); err == nil {
		t.Fatal("first run should fail")
	}
	if _, err := RunStep(ctx, "flaky", step); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2 (failures must not checkpoint)", calls)
	}
}

func TestRunStepRaceLoserGetsWinningCheckpoint(t *testing.T) {
	store := &workerTestStore{getStepMisses: true}
	if _, err := store.SaveStep(context.Background(), "job-1", "worker-a", "charge", json.RawMessage(`{"winner":true}`)); err != nil {
		t.Fatalf("seed winning checkpoint: %v", err)
	}

	// The loser of a duplicate-execution race: its GetStep missed (checkpoint
	// landed after), it computed its own result, and SaveStep hit the
	// conflict. RunStep must return the persisted winner, not the local value.
	loser := WithStepRunner(context.Background(), store, "job-1", "worker-b")
	result, err := RunStep(loser, "charge", func(context.Context) (json.RawMessage, error) {
		return json.RawMessage(`{"winner":false}`), nil
	})
	if err != nil {
		t.Fatalf("run step: %v", err)
	}
	if string(result) != `{"winner":true}` {
		t.Fatalf("result = %s, want the persisted winning checkpoint", result)
	}
}

func TestWorkerRetryResumesAfterCompletedSteps(t *testing.T) {
	store := &workerTestStore{}
	registry := NewRegistry()
	stepACalls := 0
	attempt := 0
	registry.Register("two-step", func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		attempt++
		if _, err := RunStep(ctx, "a", func(context.Context) (json.RawMessage, error) {
			stepACalls++
			return json.RawMessage(`{"a":true}`), nil
		}); err != nil {
			return nil, err
		}
		return RunStep(ctx, "b", func(context.Context) (json.RawMessage, error) {
			if attempt == 1 {
				return nil, errors.New("transient")
			}
			return json.RawMessage(`{"b":true}`), nil
		})
	})
	worker := &Worker{
		ID:            "worker-a",
		Store:         store,
		Registry:      registry,
		LeaseDuration: time.Minute,
	}

	job := Job{ID: "job-1", WorkflowType: "two-step", Payload: []byte(`{}`)}
	worker.runJob(context.Background(), job)
	if store.failedJobID != "job-1" {
		t.Fatalf("first attempt should fail, got failedJobID = %q", store.failedJobID)
	}

	worker.runJob(context.Background(), job)
	if !store.completed {
		t.Fatal("second attempt should complete")
	}
	if stepACalls != 1 {
		t.Fatalf("step a ran %d times, want 1 (retry must resume after checkpoint)", stepACalls)
	}
}
