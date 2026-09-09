package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stemitom/workrail/internal/engine"
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

	if err := store.Heartbeat(ctx, enqueued.ID, "worker-a", time.Minute); err != nil {
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

	depths, err := store.QueueDepth(ctx)
	if err != nil {
		t.Fatalf("queue depth: %v", err)
	}
	if !hasQueueDepth(depths, "emails", string(engine.StatusRunning), 1) {
		t.Fatalf("depths = %+v, want emails running count", depths)
	}
	if !hasQueueDepth(depths, "billing", string(engine.StatusQueued), 1) {
		t.Fatalf("depths = %+v, want billing queued count", depths)
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

func TestIntegrationHeartbeatExtendsConfiguredLease(t *testing.T) {
	store, ctx := integrationStore(t)

	enqueued, _, err := store.Enqueue(ctx, engine.EnqueueRequest{
		WorkflowType: "echo",
		Payload:      []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := store.Claim(ctx, engine.ClaimOptions{
		WorkerID:      "worker-a",
		LeaseDuration: 2 * time.Minute,
		Limit:         1,
	}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := store.Heartbeat(ctx, enqueued.ID, "worker-a", 2*time.Minute); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	job, _, err := store.Get(ctx, enqueued.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if job.LeaseExpiresAt == nil {
		t.Fatal("lease_expires_at is nil after heartbeat")
	}
	if remaining := time.Until(*job.LeaseExpiresAt); remaining < time.Minute {
		t.Fatalf("lease remaining = %s, want ~2m; heartbeat did not honor configured lease duration", remaining)
	}
}

func TestIntegrationExhaustedExpiredLeaseDeadLetters(t *testing.T) {
	store, ctx := integrationStore(t)

	enqueued, _, err := store.Enqueue(ctx, engine.EnqueueRequest{
		WorkflowType: "echo",
		Payload:      []byte(`{}`),
		MaxAttempts:  1,
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
	if len(second) != 0 {
		t.Fatalf("second claim = %+v, want none; exhausted job must not be reclaimed", second)
	}

	ids, err := store.DeadLetterExhausted(ctx)
	if err != nil {
		t.Fatalf("dead letter exhausted: %v", err)
	}
	if len(ids) != 1 || ids[0] != enqueued.ID {
		t.Fatalf("dead-lettered %v, want [%s]", ids, enqueued.ID)
	}

	job, events, err := store.Get(ctx, enqueued.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if job.Status != engine.StatusDeadLetter {
		t.Fatalf("status = %s, want %s", job.Status, engine.StatusDeadLetter)
	}
	if job.Error == nil || *job.Error == "" {
		t.Fatal("dead-lettered job should record an error")
	}
	found := false
	for _, event := range events {
		if event.EventType == "job.dead_lettered" {
			found = true
		}
	}
	if !found {
		t.Fatalf("events = %+v, want a job.dead_lettered event", events)
	}
}

func TestIntegrationListKeysetPagination(t *testing.T) {
	store, ctx := integrationStore(t)

	var ids []string
	for i := range 3 {
		job, _, err := store.Enqueue(ctx, engine.EnqueueRequest{
			WorkflowType: "echo",
			Payload:      []byte(`{}`),
			RunAfter:     time.Now().UTC().Add(time.Duration(i) * time.Hour),
		})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		ids = append(ids, job.ID)
		time.Sleep(5 * time.Millisecond)
	}

	first, err := store.List(ctx, engine.ListOptions{Limit: 2})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if len(first) != 2 || first[0].ID != ids[2] || first[1].ID != ids[1] {
		t.Fatalf("first page = %v, want newest two", jobIDs(first))
	}

	second, err := store.List(ctx, engine.ListOptions{
		Limit:           2,
		BeforeCreatedAt: first[1].CreatedAt,
		BeforeID:        first[1].ID,
	})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if len(second) != 1 || second[0].ID != ids[0] {
		t.Fatalf("second page = %v, want only the oldest job", jobIDs(second))
	}
}

func jobIDs(jobs []engine.Job) []string {
	ids := make([]string, len(jobs))
	for i, job := range jobs {
		ids[i] = job.ID
	}
	return ids
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

func TestIntegrationStepCheckpoints(t *testing.T) {
	store, ctx := integrationStore(t)

	enqueued, _, err := store.Enqueue(ctx, engine.EnqueueRequest{
		WorkflowType: "two-step",
		Payload:      []byte(`{}`),
		MaxAttempts:  1,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := store.Claim(ctx, engine.ClaimOptions{
		WorkerID:      "worker-a",
		LeaseDuration: time.Minute,
		Limit:         1,
	}); err != nil {
		t.Fatalf("claim: %v", err)
	}

	if _, found, err := store.GetStep(ctx, enqueued.ID, "charge"); err != nil || found {
		t.Fatalf("get before save: found=%v err=%v, want absent", found, err)
	}
	if _, err := store.SaveStep(ctx, enqueued.ID, "worker-b", "charge", []byte(`{"amount":1}`)); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("save from non-owner: err = %v, want ErrInvalidTransition", err)
	}
	if _, err := store.SaveStep(ctx, enqueued.ID, "worker-a", "charge", []byte(`{"amount":42}`)); err != nil {
		t.Fatalf("save step: %v", err)
	}
	stored, err := store.SaveStep(ctx, enqueued.ID, "worker-a", "charge", []byte(`{"amount":99}`))
	if err != nil {
		t.Fatalf("conflicting save: %v", err)
	}
	if string(stored) != `{"amount": 42}` && string(stored) != `{"amount":42}` {
		t.Fatalf("conflicting save returned %s, want the winning first write", stored)
	}
	result, found, err := store.GetStep(ctx, enqueued.ID, "charge")
	if err != nil || !found {
		t.Fatalf("get step: found=%v err=%v", found, err)
	}
	if string(result) != `{"amount": 42}` && string(result) != `{"amount":42}` {
		t.Fatalf("step result = %s, want the first write to win", result)
	}

	if err := store.Fail(ctx, enqueued.ID, "worker-a", errors.New("boom")); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if _, err := store.RetryDeadLetter(ctx, enqueued.ID); err != nil {
		t.Fatalf("dlq retry: %v", err)
	}
	if _, found, err := store.GetStep(ctx, enqueued.ID, "charge"); err != nil || !found {
		t.Fatalf("step must survive dlq retry so the job resumes: found=%v err=%v", found, err)
	}

	_, events, err := store.Get(ctx, enqueued.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	stepEvents := 0
	for _, event := range events {
		if event.EventType == "job.step_completed" {
			stepEvents++
		}
	}
	if stepEvents != 1 {
		t.Fatalf("job.step_completed events = %d, want 1 (conflict save must not re-log)", stepEvents)
	}
}

func TestIntegrationPruneCompleted(t *testing.T) {
	store, ctx := integrationStore(t)

	succeeded, _, err := store.Enqueue(ctx, engine.EnqueueRequest{
		WorkflowType: "echo",
		Payload:      []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := store.Claim(ctx, engine.ClaimOptions{
		WorkerID:      "worker-a",
		LeaseDuration: time.Minute,
		Limit:         1,
	}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := store.Complete(ctx, succeeded.ID, "worker-a", []byte(`{}`)); err != nil {
		t.Fatalf("complete: %v", err)
	}

	deadLettered, _, err := store.Enqueue(ctx, engine.EnqueueRequest{
		WorkflowType: "echo",
		Payload:      []byte(`{}`),
		MaxAttempts:  1,
	})
	if err != nil {
		t.Fatalf("enqueue dead letter: %v", err)
	}
	if _, err := store.Claim(ctx, engine.ClaimOptions{
		WorkerID:      "worker-a",
		LeaseDuration: time.Minute,
		Limit:         1,
	}); err != nil {
		t.Fatalf("claim dead letter: %v", err)
	}
	if err := store.Fail(ctx, deadLettered.ID, "worker-a", errors.New("boom")); err != nil {
		t.Fatalf("fail: %v", err)
	}

	if _, err := store.db.Exec(ctx, "UPDATE jobs SET completed_at = now() - interval '48 hours'"); err != nil {
		t.Fatalf("age jobs: %v", err)
	}

	count, err := store.PruneCompleted(ctx, "default", 24*time.Hour)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if count != 1 {
		t.Fatalf("pruned %d jobs, want 1", count)
	}
	if _, _, err := store.Get(ctx, succeeded.ID); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("get succeeded job after prune: err = %v, want ErrNotFound", err)
	}
	if job, _, err := store.Get(ctx, deadLettered.ID); err != nil || job.Status != engine.StatusDeadLetter {
		t.Fatalf("dead-lettered job must survive pruning; job = %+v, err = %v", job, err)
	}
}

func hasQueueDepth(depths []engine.QueueDepth, queue, status string, count int64) bool {
	for _, depth := range depths {
		if depth.Queue == queue && depth.Status == status && depth.Count == count {
			return true
		}
	}
	return false
}

func TestIntegrationPermanentFailureSkipsRetries(t *testing.T) {
	store, ctx := integrationStore(t)

	enqueued, _, err := store.Enqueue(ctx, engine.EnqueueRequest{
		WorkflowType: "payout",
		Payload:      []byte(`{}`),
		MaxAttempts:  5,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := store.Claim(ctx, engine.ClaimOptions{
		WorkerID:      "worker-a",
		LeaseDuration: time.Minute,
		Limit:         1,
	}); err != nil {
		t.Fatalf("claim: %v", err)
	}

	cause := engine.Permanent(errors.New("account closed"))
	if err := store.Fail(ctx, enqueued.ID, "worker-a", cause); err != nil {
		t.Fatalf("fail: %v", err)
	}

	job, events, err := store.Get(ctx, enqueued.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	// Four attempts remained; a permanent cause must not spend them.
	if job.Status != engine.StatusDeadLetter {
		t.Fatalf("status = %s, want %s", job.Status, engine.StatusDeadLetter)
	}
	if job.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", job.Attempt)
	}
	if job.Error == nil || *job.Error != "account closed" {
		t.Fatalf("error = %v, want account closed", job.Error)
	}

	var failed bool
	for _, event := range events {
		if event.EventType != "job.failed" {
			continue
		}
		failed = true
		var details struct {
			Permanent  bool   `json:"permanent"`
			NextStatus string `json:"next_status"`
		}
		if err := json.Unmarshal(event.Details, &details); err != nil {
			t.Fatalf("decode job.failed details: %v", err)
		}
		if !details.Permanent {
			t.Fatalf("job.failed details = %s, want permanent flag", event.Details)
		}
	}
	if !failed {
		t.Fatal("no job.failed event recorded")
	}
}

func TestIntegrationSignalUnknownJobIsNotFound(t *testing.T) {
	store, ctx := integrationStore(t)

	err := store.Signal(ctx, "00000000-0000-0000-0000-000000000000", "approval", []byte(`{}`), "")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("signal to unknown job = %v, want ErrNotFound", err)
	}
}

func TestIntegrationSignalIdempotencyKeyDedupes(t *testing.T) {
	store, ctx := integrationStore(t)

	enqueued, _, err := store.Enqueue(ctx, engine.EnqueueRequest{WorkflowType: "approval"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	for range 2 {
		if err := store.Signal(ctx, enqueued.ID, "vote", []byte(`{"n":1}`), "req-1"); err != nil {
			t.Fatalf("signal: %v", err)
		}
	}
	if err := store.Signal(ctx, enqueued.ID, "vote", []byte(`{"n":2}`), "req-2"); err != nil {
		t.Fatalf("signal: %v", err)
	}

	var count int
	if err := store.db.QueryRow(ctx, `SELECT count(*) FROM job_signals WHERE job_id = $1`, enqueued.ID).Scan(&count); err != nil {
		t.Fatalf("count signals: %v", err)
	}
	if count != 2 {
		t.Fatalf("mailbox holds %d rows, want 2 unique sends", count)
	}
	first, found, err := store.GetSignalAt(ctx, enqueued.ID, "vote", 0)
	if err != nil || !found {
		t.Fatalf("get signal: found = %v, err = %v", found, err)
	}
	var delivered struct {
		N int `json:"n"`
	}
	if json.Unmarshal(first, &delivered) != nil || delivered.N != 1 {
		t.Fatalf("payload = %s, want first send", first)
	}
}

func TestIntegrationStaleSuspendBacksOff(t *testing.T) {
	store, ctx := integrationStore(t)

	enqueued, _, err := store.Enqueue(ctx, engine.EnqueueRequest{WorkflowType: "approval"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimed, err := store.Claim(ctx, engine.ClaimOptions{WorkerID: "worker-a", LeaseDuration: time.Minute, Limit: 1})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v", err)
	}
	// A signal lands after the claim: the waiter's mailbox check already ran.
	if err := store.Signal(ctx, enqueued.ID, "approval", []byte(`{}`), ""); err != nil {
		t.Fatalf("signal: %v", err)
	}

	parked, err := store.Suspend(ctx, enqueued.ID, "worker-a", time.Now().UTC().Add(5*time.Minute), claimed[0].WakeVersion)
	if err != nil {
		t.Fatalf("suspend: %v", err)
	}
	if parked {
		t.Fatal("stale Suspend parked over a signal wake-up")
	}
	// The job is untouched: still running, still owned, still claimable later.
	job, _, err := store.Get(ctx, enqueued.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if job.Status != engine.StatusRunning {
		t.Fatalf("status = %s, want running", job.Status)
	}

	parked, err = store.Suspend(ctx, enqueued.ID, "worker-a", time.Now().UTC().Add(5*time.Minute), job.WakeVersion)
	if err != nil || !parked {
		t.Fatalf("fresh Suspend parked = %v, err = %v, want park", parked, err)
	}
}

func TestIntegrationSignalDeliverAndRejectTerminal(t *testing.T) {
	store, ctx := integrationStore(t)

	enqueued, _, err := store.Enqueue(ctx, engine.EnqueueRequest{
		WorkflowType: "approval",
		Payload:      []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := store.Claim(ctx, engine.ClaimOptions{
		WorkerID:      "worker-a",
		LeaseDuration: time.Minute,
		Limit:         1,
	}); err != nil {
		t.Fatalf("claim: %v", err)
	}

	if err := store.Signal(ctx, enqueued.ID, "approval", []byte(`{"ok":true}`), ""); err != nil {
		t.Fatalf("signal: %v", err)
	}
	if err := store.Signal(ctx, enqueued.ID, "approval", []byte(`{"ok":false}`), ""); err != nil {
		t.Fatalf("second signal: %v", err)
	}
	payload, found, err := store.GetSignalAt(ctx, enqueued.ID, "approval", 0)
	if err != nil {
		t.Fatalf("get signal: %v", err)
	}
	var delivered struct {
		OK bool `json:"ok"`
	}
	if !found || json.Unmarshal(payload, &delivered) != nil || !delivered.OK {
		t.Fatalf("payload = %s, found = %v, want earliest delivery", payload, found)
	}
	second, found, err := store.GetSignalAt(ctx, enqueued.ID, "approval", 1)
	if err != nil {
		t.Fatalf("get second signal: %v", err)
	}
	var later struct {
		OK bool `json:"ok"`
	}
	if !found || json.Unmarshal(second, &later) != nil || later.OK {
		t.Fatalf("second = %s, found = %v, want later delivery", second, found)
	}
	if _, found, err := store.GetSignalAt(ctx, enqueued.ID, "other", 0); err != nil || found {
		t.Fatalf("unknown signal found = %v, err = %v, want miss", found, err)
	}

	// Terminal jobs reject signals: nobody is waiting anymore.
	if err := store.Complete(ctx, enqueued.ID, "worker-a", []byte(`{}`)); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := store.Signal(ctx, enqueued.ID, "approval", []byte(`{}`), ""); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("signal to finished job = %v, want ErrInvalidTransition", err)
	}

	_, events, err := store.Get(ctx, enqueued.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	var signaled bool
	for _, event := range events {
		if event.EventType == "job.signaled" {
			signaled = true
		}
	}
	if !signaled {
		t.Fatal("no job.signaled event recorded")
	}

	for _, terminal := range []struct {
		name   string
		finish func(ctx context.Context, store *Store, job engine.Job) error
	}{
		{"canceled", func(ctx context.Context, store *Store, job engine.Job) error {
			return store.Cancel(ctx, job.ID)
		}},
		{"dead_letter", func(ctx context.Context, store *Store, job engine.Job) error {
			if _, err := store.Claim(ctx, engine.ClaimOptions{WorkerID: "worker-a", LeaseDuration: time.Minute, Limit: 1}); err != nil {
				return err
			}
			return store.Fail(ctx, job.ID, "worker-a", errors.New("boom"))
		}},
	} {
		t.Run(terminal.name, func(t *testing.T) {
			job, _, err := store.Enqueue(ctx, engine.EnqueueRequest{WorkflowType: "approval", MaxAttempts: 1})
			if err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			if err := terminal.finish(ctx, store, job); err != nil {
				t.Fatalf("finish: %v", err)
			}
			if err := store.Signal(ctx, job.ID, "approval", []byte(`{}`), ""); !errors.Is(err, engine.ErrInvalidTransition) {
				t.Fatalf("signal to %s job = %v, want ErrInvalidTransition", terminal.name, err)
			}
		})
	}
}

func TestIntegrationChildRoundTrip(t *testing.T) {
	store, ctx := integrationStore(t)

	parent, _, err := store.Enqueue(ctx, engine.EnqueueRequest{WorkflowType: "payout"})
	if err != nil {
		t.Fatalf("enqueue parent: %v", err)
	}
	if _, err := store.Claim(ctx, engine.ClaimOptions{WorkerID: "worker-a", LeaseDuration: time.Minute, Limit: 10}); err != nil {
		t.Fatalf("claim parent: %v", err)
	}
	runnerCtx := engine.WithStepRunner(ctx, store, parent.ID, "worker-a", "default")
	runnerCtx = engine.WithRegistry(runnerCtx, engine.NewRegistry())

	_, err = engine.ExecuteChild(runnerCtx, "settle", "settlement", []byte(`{}`))
	var suspend *engine.SuspendError
	if !errors.As(err, &suspend) {
		t.Fatalf("first run = %v, want suspend", err)
	}

	children, err := store.List(ctx, engine.ListOptions{WorkflowType: "settlement", Limit: 10})
	if err != nil || len(children) != 1 {
		t.Fatalf("children = %+v, err = %v, want exactly 1", children, err)
	}
	child := children[0]
	if child.Queue != "default" {
		t.Fatalf("child queue = %q, want parent queue", child.Queue)
	}
	if _, err := store.Claim(ctx, engine.ClaimOptions{WorkerID: "worker-b", Queue: "default", LeaseDuration: time.Minute, Limit: 10}); err != nil {
		t.Fatalf("claim child: %v", err)
	}
	if err := store.Complete(ctx, child.ID, "worker-b", []byte(`{"status":"paid"}`)); err != nil {
		t.Fatalf("complete child: %v", err)
	}

	// Same identity on retry: no second child row appears.
	if _, err := engine.ExecuteChild(runnerCtx, "settle", "settlement", []byte(`{}`)); err != nil {
		t.Fatalf("second run = %v, want child result", err)
	}
	children, err = store.List(ctx, engine.ListOptions{WorkflowType: "settlement", Limit: 10})
	if err != nil || len(children) != 1 {
		t.Fatalf("children = %d, want exactly 1 (stable identity)", len(children))
	}
}

func TestIntegrationRecordEvent(t *testing.T) {
	store, ctx := integrationStore(t)

	enqueued, _, err := store.Enqueue(ctx, engine.EnqueueRequest{WorkflowType: "payout"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := store.RecordEvent(ctx, enqueued.ID, "job.compensated", []byte(`{"by":"saga"}`)); err != nil {
		t.Fatalf("record: %v", err)
	}
	_, events, err := store.Get(ctx, enqueued.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	for _, event := range events {
		if event.EventType == "job.compensated" {
			return
		}
	}
	t.Fatal("no job.compensated event recorded")
}

func TestIntegrationParentLineage(t *testing.T) {
	store, ctx := integrationStore(t)

	parent, _, err := store.Enqueue(ctx, engine.EnqueueRequest{WorkflowType: "payout"})
	if err != nil {
		t.Fatalf("enqueue parent: %v", err)
	}
	child, _, err := store.Enqueue(ctx, engine.EnqueueRequest{
		WorkflowType: "settlement",
		ParentID:     parent.ID,
	})
	if err != nil {
		t.Fatalf("enqueue child: %v", err)
	}

	got, err := store.GetJob(ctx, child.ID)
	if err != nil {
		t.Fatalf("get child: %v", err)
	}
	if got.ParentID == nil || *got.ParentID != parent.ID {
		t.Fatalf("child parent = %v, want %s", got.ParentID, parent.ID)
	}

	children, err := store.List(ctx, engine.ListOptions{ParentID: parent.ID, Limit: 10})
	if err != nil || len(children) != 1 || children[0].ID != child.ID {
		t.Fatalf("children = %+v, err = %v, want [%s]", children, err, child.ID)
	}
	none, err := store.List(ctx, engine.ListOptions{ParentID: child.ID, Limit: 10})
	if err != nil || len(none) != 0 {
		t.Fatalf("leaf children = %+v, want none", none)
	}
}

func TestIntegrationListSignalsOrdered(t *testing.T) {
	store, ctx := integrationStore(t)

	job, _, err := store.Enqueue(ctx, engine.EnqueueRequest{WorkflowType: "approval"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	for _, name := range []string{"first", "second"} {
		if err := store.Signal(ctx, job.ID, name, []byte(`{}`), ""); err != nil {
			t.Fatalf("signal %s: %v", name, err)
		}
	}
	signals, err := store.ListSignals(ctx, job.ID)
	if err != nil {
		t.Fatalf("list signals: %v", err)
	}
	if len(signals) != 2 || signals[0].Name != "first" || signals[1].Name != "second" {
		t.Fatalf("signals = %+v, want delivery order", signals)
	}
}
