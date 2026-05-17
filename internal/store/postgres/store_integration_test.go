package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"workrail/internal/engine"
)

func integrationStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run postgres integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	store, err := New(ctx, url)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(store.Close)
	resetDB(t, ctx, store)
	return store, ctx
}

func resetDB(t *testing.T, ctx context.Context, store *Store) {
	t.Helper()
	if _, err := store.db.Exec(ctx, "TRUNCATE job_events, jobs RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("reset db: %v", err)
	}
}

func TestIntegrationEnqueueIdempotency(t *testing.T) {
	store, ctx := integrationStore(t)

	first, inserted, err := store.Enqueue(ctx, engine.EnqueueRequest{
		WorkflowType:   "echo",
		Payload:        []byte(`{"message":"first"}`),
		IdempotencyKey: "same-key",
	})
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	if !inserted {
		t.Fatal("first enqueue should insert")
	}

	second, inserted, err := store.Enqueue(ctx, engine.EnqueueRequest{
		WorkflowType:   "echo",
		Payload:        []byte(`{"message":"second"}`),
		IdempotencyKey: "same-key",
	})
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if inserted {
		t.Fatal("second enqueue should hit existing idempotency key")
	}
	if second.ID != first.ID {
		t.Fatalf("idempotency returned job %s, want %s", second.ID, first.ID)
	}
}

func TestIntegrationClaimCompleteLifecycle(t *testing.T) {
	store, ctx := integrationStore(t)

	enqueued, _, err := store.Enqueue(ctx, engine.EnqueueRequest{
		WorkflowType: "echo",
		Payload:      []byte(`{"ok":true}`),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	claimed, err := store.Claim(ctx, engine.ClaimOptions{
		WorkerID:      "worker-a",
		LeaseDuration: time.Minute,
		Limit:         1,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != enqueued.ID {
		t.Fatalf("claimed jobs = %+v, want %s", claimed, enqueued.ID)
	}
	if claimed[0].Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", claimed[0].Attempt)
	}

	if err := store.Heartbeat(ctx, enqueued.ID, "worker-a"); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if err := store.Complete(ctx, enqueued.ID, "worker-a", []byte(`{"done":true}`)); err != nil {
		t.Fatalf("complete: %v", err)
	}

	job, events, err := store.Get(ctx, enqueued.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if job.Status != engine.StatusSucceeded {
		t.Fatalf("status = %s, want %s", job.Status, engine.StatusSucceeded)
	}
	if len(events) < 4 {
		t.Fatalf("events = %d, want at least 4", len(events))
	}
}

func TestIntegrationClaimRespectsQueue(t *testing.T) {
	store, ctx := integrationStore(t)

	emailJob, _, err := store.Enqueue(ctx, engine.EnqueueRequest{
		Queue:        "emails",
		WorkflowType: "echo",
		Payload:      []byte(`{"queue":"emails"}`),
	})
	if err != nil {
		t.Fatalf("enqueue emails: %v", err)
	}
	billingJob, _, err := store.Enqueue(ctx, engine.EnqueueRequest{
		Queue:        "billing",
		WorkflowType: "echo",
		Payload:      []byte(`{"queue":"billing"}`),
	})
	if err != nil {
		t.Fatalf("enqueue billing: %v", err)
	}

	claimed, err := store.Claim(ctx, engine.ClaimOptions{
		WorkerID:      "worker-emails",
		Queue:         "emails",
		LeaseDuration: time.Minute,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("claim emails: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != emailJob.ID {
		t.Fatalf("claimed = %+v, want only %s", claimed, emailJob.ID)
	}
	if claimed[0].Queue != "emails" {
		t.Fatalf("claimed queue = %s, want emails", claimed[0].Queue)
	}

	billingJobs, err := store.List(ctx, engine.ListOptions{Queue: "billing"})
	if err != nil {
		t.Fatalf("list billing: %v", err)
	}
	if len(billingJobs) != 1 || billingJobs[0].ID != billingJob.ID {
		t.Fatalf("billing list = %+v, want %s", billingJobs, billingJob.ID)
	}
}

func TestIntegrationRetryThenDeadLetter(t *testing.T) {
	store, ctx := integrationStore(t)

	enqueued, _, err := store.Enqueue(ctx, engine.EnqueueRequest{
		WorkflowType: "missing",
		Payload:      []byte(`{}`),
		MaxAttempts:  1,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimed, err := store.Claim(ctx, engine.ClaimOptions{
		WorkerID:      "worker-a",
		LeaseDuration: time.Minute,
		Limit:         1,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d jobs, want 1", len(claimed))
	}
	if err := store.Fail(ctx, enqueued.ID, "worker-a", errors.New("boom")); err != nil {
		t.Fatalf("fail: %v", err)
	}

	job, _, err := store.Get(ctx, enqueued.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if job.Status != engine.StatusDeadLetter {
		t.Fatalf("status = %s, want %s", job.Status, engine.StatusDeadLetter)
	}
	if job.Error == nil || *job.Error != "boom" {
		t.Fatalf("error = %v, want boom", job.Error)
	}

	deadLetters, err := store.List(ctx, engine.ListOptions{Status: engine.StatusDeadLetter})
	if err != nil {
		t.Fatalf("list dead letters: %v", err)
	}
	if len(deadLetters) != 1 || deadLetters[0].ID != enqueued.ID {
		t.Fatalf("dead letters = %+v, want %s", deadLetters, enqueued.ID)
	}

	retried, err := store.RetryDeadLetter(ctx, enqueued.ID)
	if err != nil {
		t.Fatalf("retry dead letter: %v", err)
	}
	if retried.Status != engine.StatusQueued {
		t.Fatalf("retried status = %s, want %s", retried.Status, engine.StatusQueued)
	}
	if retried.Attempt != 0 {
		t.Fatalf("retried attempt = %d, want 0", retried.Attempt)
	}
}

func TestIntegrationExpiredLeaseCanBeReclaimed(t *testing.T) {
	store, ctx := integrationStore(t)

	enqueued, _, err := store.Enqueue(ctx, engine.EnqueueRequest{
		WorkflowType: "echo",
		Payload:      []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	first, err := store.Claim(ctx, engine.ClaimOptions{
		WorkerID:      "worker-a",
		LeaseDuration: time.Nanosecond,
		Limit:         1,
	})
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("first claim got %d jobs, want 1", len(first))
	}

	time.Sleep(5 * time.Millisecond)
	second, err := store.Claim(ctx, engine.ClaimOptions{
		WorkerID:      "worker-b",
		LeaseDuration: time.Minute,
		Limit:         1,
	})
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if len(second) != 1 || second[0].ID != enqueued.ID {
		t.Fatalf("second claim = %+v, want %s", second, enqueued.ID)
	}
	if second[0].Attempt != 2 {
		t.Fatalf("attempt = %d, want 2 after reclaim", second[0].Attempt)
	}
}

func TestIntegrationCancelAndReplay(t *testing.T) {
	store, ctx := integrationStore(t)

	enqueued, _, err := store.Enqueue(ctx, engine.EnqueueRequest{
		WorkflowType: "echo",
		Payload:      []byte(`{"version":1}`),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := store.Cancel(ctx, enqueued.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	canceled, _, err := store.Get(ctx, enqueued.ID)
	if err != nil {
		t.Fatalf("get canceled: %v", err)
	}
	if canceled.Status != engine.StatusCanceled {
		t.Fatalf("status = %s, want %s", canceled.Status, engine.StatusCanceled)
	}

	replayed, err := store.Replay(ctx, enqueued.ID)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed.ID == enqueued.ID {
		t.Fatal("replay should create a new job")
	}
	if replayed.Status != engine.StatusQueued {
		t.Fatalf("replayed status = %s, want %s", replayed.Status, engine.StatusQueued)
	}
}
