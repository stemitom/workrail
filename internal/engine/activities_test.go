package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func activityTestContext(store *workerTestStore, reg *Registry) context.Context {
	return WithRegistry(WithStepRunner(context.Background(), store, "job-1", "worker-a"), reg)
}

func TestActivitiesScopeStepCheckpoints(t *testing.T) {
	store := &workerTestStore{}
	reg := NewRegistry()
	calls := map[string]int{}
	mkActivity := func(tag string) ActivityFunc {
		return func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
			return RunStep(ctx, "x", func(context.Context) (json.RawMessage, error) {
				calls[tag]++
				return json.RawMessage(`{"tag":"` + tag + `"}`), nil
			})
		}
	}
	reg.RegisterActivity("a", mkActivity("a"))
	reg.RegisterActivity("b", mkActivity("b"))
	ctx := WithRegistry(WithStepRunner(context.Background(), store, "job-1", "worker-a"), reg)

	first, err := ExecuteActivity(ctx, "a", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("activity a: %v", err)
	}
	second, err := ExecuteActivity(ctx, "b", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("activity b: %v", err)
	}
	// Same step name, independent checkpoints: both ran, distinct results.
	if calls["a"] != 1 || calls["b"] != 1 {
		t.Fatalf("calls = %v, want one run per activity", calls)
	}
	if string(first) == string(second) {
		t.Fatalf("results collided: %s", first)
	}
	if _, ok := store.steps["job-1/x"]; ok {
		t.Fatal("top-level checkpoint job-1/x must not exist")
	}
	if _, ok := store.steps["job-1/a/x"]; !ok {
		t.Fatal("missing scoped checkpoint job-1/a/x")
	}

	// Re-running replays the checkpoint instead of the side effect.
	if _, err := ExecuteActivity(ctx, "a", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("activity a rerun: %v", err)
	}
	if calls["a"] != 1 {
		t.Fatalf("calls[a] = %d, want 1 (checkpoint replay)", calls["a"])
	}
}

func TestExecuteActivityNeedsWorkerAndRegistry(t *testing.T) {
	if _, err := ExecuteActivity(context.Background(), "x", json.RawMessage(`{}`)); err == nil {
		t.Fatal("ExecuteActivity without a runner should fail")
	}
	runnerOnly := WithStepRunner(context.Background(), &workerTestStore{}, "job-1", "worker-a")
	if _, err := ExecuteActivity(runnerOnly, "echo", json.RawMessage(`{}`)); err == nil {
		t.Fatal("ExecuteActivity without a registry should fail")
	}
	if _, err := ExecuteActivity(activityTestContext(&workerTestStore{}, NewRegistry()), "nope", json.RawMessage(`{}`)); err == nil ||
		!strings.Contains(err.Error(), `unknown activity "nope"`) {
		t.Fatalf("unknown activity err = %v", err)
	}
}

func TestWorkerRunsActivityTypedJob(t *testing.T) {
	store := &workerTestStore{}
	registry := NewRegistry() // echo is activity-only; jobs naming it still run
	worker := &Worker{
		ID:            "worker-a",
		Store:         store,
		Registry:      registry,
		LeaseDuration: time.Minute,
	}

	worker.runJob(context.Background(), Job{ID: "job-1", WorkflowType: "echo", Payload: []byte(`{"message":"hi"}`)})

	if store.completedJobID != "job-1" {
		t.Fatalf("completed job = %q, want job-1", store.completedJobID)
	}
}

func TestSequenceRunsScopedActivities(t *testing.T) {
	store := &workerTestStore{}
	registry := NewRegistry()
	// A step-ful activity: two sequence steps calling it must not share its
	// inner checkpoint.
	registry.RegisterActivity("tagger", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return RunStep(ctx, "x", func(context.Context) (json.RawMessage, error) {
			return input, nil
		})
	})
	worker := &Worker{
		ID:            "worker-a",
		Store:         store,
		Registry:      registry,
		LeaseDuration: time.Minute,
	}
	payload := []byte(`{"steps":[
		{"name":"first","activity":"echo","input":{"message":"hi"}},
		{"name":"second","activity":"tagger","input":{"message":"yo"}},
		{"name":"third","activity":"tagger","input":{"message":"hey"}}
	]}`)

	worker.runJob(context.Background(), Job{ID: "job-1", WorkflowType: "sequence", Payload: payload})

	if store.completedJobID != "job-1" {
		t.Fatalf("completed job = %q, want job-1 (failed: %v)", store.completedJobID, store.failedErr)
	}
	for _, step := range []string{"job-1/first", "job-1/second", "job-1/third"} {
		if _, ok := store.steps[step]; !ok {
			t.Fatalf("missing sequence checkpoint %s", step)
		}
	}
	// Inner checkpoints live under their spec step, with their own results.
	second, ok := store.steps["job-1/second/tagger/x"]
	third, ok2 := store.steps["job-1/third/tagger/x"]
	if !ok || !ok2 || string(second) == string(third) {
		t.Fatalf("inner checkpoints = %s, %s, want distinct per-step results", second, third)
	}
}

func TestWorkflowsAndActivitiesDoNotShareNames(t *testing.T) {
	reg := NewRegistry()
	reg.Register("w", func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"w":true}`), nil
	})
	reg.RegisterActivity("a", func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"a":true}`), nil
	})
	ctx := activityTestContext(&workerTestStore{}, reg)

	if _, err := ExecuteActivity(ctx, "w", json.RawMessage(`{}`)); err == nil {
		t.Fatal("workflows must not run as activities")
	}
	if _, err := reg.Execute(ctx, "zzz", json.RawMessage(`{}`)); err == nil ||
		!strings.Contains(err.Error(), `unknown workflow type "zzz"`) {
		t.Fatalf("unknown type err = %v", err)
	}
}
