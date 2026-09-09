package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func childTestContext(store *workerTestStore) context.Context {
	return WithRegistry(WithStepRunner(context.Background(), store, "parent-1", "worker-a", "default"), NewRegistry())
}

func TestExecuteChildStableIdentity(t *testing.T) {
	store := &workerTestStore{}
	ctx := childTestContext(store)

	first, err := ExecuteChild(ctx, "settle", "settlement", json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("first run should suspend waiting for the child")
	} else if first != nil {
		t.Fatalf("suspended run returned %s, want nothing", first)
	}
	var suspend *SuspendError
	if !errors.As(err, &suspend) {
		t.Fatalf("first run = %v, want suspend", err)
	}
	second, err := ExecuteChild(ctx, "settle", "settlement", json.RawMessage(`{}`))
	if !errors.As(err, &suspend) {
		t.Fatalf("second run = %v, %v, want suspend (same child)", second, err)
	}
	if len(store.jobs) != 1 {
		t.Fatalf("%d child jobs, want exactly 1 (stable identity)", len(store.jobs))
	}
	for _, job := range store.jobs {
		if job.Queue != "default" || job.WorkflowType != "settlement" {
			t.Fatalf("child = %+v, want default/settlement", job)
		}
	}
}

func TestExecuteChildReturnsResult(t *testing.T) {
	store := &workerTestStore{}
	ctx := childTestContext(store)

	if _, err := ExecuteChild(ctx, "settle", "settlement", json.RawMessage(`{}`)); err == nil {
		t.Fatal("running child should suspend")
	}
	var childID string
	for id := range store.jobs {
		childID = id
	}
	store.setJobStatus(childID, StatusSucceeded)
	store.jobs[childID] = withResult(store.jobs[childID], `{"status":"paid"}`)

	result, err := ExecuteChild(ctx, "settle", "settlement", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("finished child: %v", err)
	}
	if string(result) != `{"status":"paid"}` {
		t.Fatalf("result = %s, want child result", result)
	}
}

func withResult(job Job, result string) Job {
	job.Result = json.RawMessage(result)
	return job
}

func TestExecuteChildTerminalFailsParentPermanently(t *testing.T) {
	for _, status := range []Status{StatusDeadLetter, StatusCanceled} {
		store := &workerTestStore{}
		ctx := childTestContext(store)
		if _, err := ExecuteChild(ctx, "settle", "settlement", json.RawMessage(`{}`)); err == nil {
			t.Fatal("running child should suspend")
		}
		for id := range store.jobs {
			store.setJobStatus(id, status)
		}
		_, err := ExecuteChild(ctx, "settle", "settlement", json.RawMessage(`{}`))
		if !IsPermanent(err) {
			t.Fatalf("%s child err = %v, want permanent", status, err)
		}
	}
}

func TestExecuteChildNeedsWorker(t *testing.T) {
	if _, err := ExecuteChild(context.Background(), "x", "y", json.RawMessage(`{}`)); err == nil {
		t.Fatal("ExecuteChild without a runner should fail")
	}
}

func TestExecuteChildPropagatesQueue(t *testing.T) {
	store := &workerTestStore{}
	ctx := WithRegistry(WithStepRunner(context.Background(), store, "parent-1", "worker-a", "payouts"), NewRegistry())
	if _, err := ExecuteChild(ctx, "settle", "settlement", json.RawMessage(`{}`)); err == nil {
		t.Fatal("running child should suspend")
	}
	for _, job := range store.jobs {
		if job.Queue != "payouts" {
			t.Fatalf("child queue = %q, want parent queue", job.Queue)
		}
	}
}

func TestCompensationRunsOnDeadLetter(t *testing.T) {
	store := &workerTestStore{}
	registry := NewRegistry()
	compensated := false
	registry.Register("payout", func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("rail down")
	})
	registry.RegisterCompensation("payout", func(_ context.Context, payload json.RawMessage) (json.RawMessage, error) {
		compensated = true
		if string(payload) != `{"id":"p1"}` {
			t.Errorf("compensator payload = %s", payload)
		}
		return json.RawMessage(`{}`), nil
	})
	worker := &Worker{ID: "worker-a", Store: store, Registry: registry, LeaseDuration: time.Minute}

	enqueued, _, err := store.Enqueue(context.Background(), EnqueueRequest{WorkflowType: "payout", Payload: []byte(`{"id":"p1"}`)})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	worker.runJob(context.Background(), Job{ID: enqueued.ID, WorkflowType: "payout", Payload: []byte(`{"id":"p1"}`)})

	if !compensated {
		t.Fatal("compensator did not run for dead-lettered job")
	}
	var compensatedEvent bool
	for _, event := range store.events {
		if event.EventType == "job.compensated" {
			compensatedEvent = true
		}
	}
	if !compensatedEvent {
		t.Fatal("no job.compensated event recorded")
	}
}

func TestCompensationFailureRecorded(t *testing.T) {
	store := &workerTestStore{}
	registry := NewRegistry()
	registry.Register("payout", func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("rail down")
	})
	registry.RegisterCompensation("payout", func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("ledger unreachable")
	})
	worker := &Worker{ID: "worker-a", Store: store, Registry: registry, LeaseDuration: time.Minute}

	enqueued, _, _ := store.Enqueue(context.Background(), EnqueueRequest{WorkflowType: "payout"})
	worker.runJob(context.Background(), Job{ID: enqueued.ID, WorkflowType: "payout", Payload: []byte(`{}`)})

	for _, event := range store.events {
		if event.EventType == "job.compensation_failed" {
			return
		}
	}
	t.Fatal("no job.compensation_failed event recorded")
}

func TestNoCompensatorIsSilent(t *testing.T) {
	store := &workerTestStore{}
	registry := NewRegistry()
	registry.Register("payout", func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("rail down")
	})
	worker := &Worker{ID: "worker-a", Store: store, Registry: registry, LeaseDuration: time.Minute}

	enqueued, _, _ := store.Enqueue(context.Background(), EnqueueRequest{WorkflowType: "payout"})
	worker.runJob(context.Background(), Job{ID: enqueued.ID, WorkflowType: "payout", Payload: []byte(`{}`)})

	if len(store.events) != 0 {
		t.Fatalf("events = %v, want none without a compensator", store.events)
	}
}

func TestSweepCompensatesExhausted(t *testing.T) {
	store := &workerTestStore{}
	registry := NewRegistry()
	compensated := false
	registry.RegisterCompensation("payout", func(context.Context, json.RawMessage) (json.RawMessage, error) {
		compensated = true
		return json.RawMessage(`{}`), nil
	})
	enqueued, _, _ := store.Enqueue(context.Background(), EnqueueRequest{WorkflowType: "payout"})
	store.setJobStatus(enqueued.ID, StatusDeadLetter)
	store.exhausted = []string{enqueued.ID}
	worker := &Worker{
		ID: "worker-a", Store: store, Registry: registry,
		PollInterval: time.Hour, LeaseDuration: 20 * time.Millisecond,
		ShutdownTimeout: time.Second, Concurrency: 1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()
	deadline := time.After(3 * time.Second)
	for !compensated {
		select {
		case <-deadline:
			t.Fatal("sweep did not compensate the exhausted job")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done
}

func TestChildErrorMentionsName(t *testing.T) {
	store := &workerTestStore{}
	ctx := childTestContext(store)
	if _, err := ExecuteChild(ctx, "settle", "settlement", json.RawMessage(`{}`)); err == nil {
		t.Fatal("running child should suspend")
	}
	for id := range store.jobs {
		store.setJobStatus(id, StatusDeadLetter)
	}
	_, err := ExecuteChild(ctx, "settle", "settlement", json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), `"settle"`) {
		t.Fatalf("child error = %v, want quoted name", err)
	}
}
