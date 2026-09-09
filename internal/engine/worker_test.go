package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWorkerRecordsPanicAsFailure(t *testing.T) {
	store := &workerTestStore{}
	registry := NewRegistry()
	registry.Register("panic", func(context.Context, json.RawMessage) (json.RawMessage, error) {
		panic("kaboom")
	})
	worker := &Worker{
		ID:            "worker-a",
		Store:         store,
		Registry:      registry,
		LeaseDuration: time.Second,
	}

	worker.runJob(context.Background(), Job{ID: "job-1", WorkflowType: "panic", Payload: []byte(`{}`)})

	if store.completed {
		t.Fatal("panic job should not complete")
	}
	if store.failedJobID != "job-1" {
		t.Fatalf("failed job = %q, want job-1", store.failedJobID)
	}
	if store.failedErr == nil || !strings.Contains(store.failedErr.Error(), "workflow panic: kaboom") {
		t.Fatalf("failure error = %v, want panic message", store.failedErr)
	}
}

func TestWorkerCancelsJobWhenLeaseLost(t *testing.T) {
	store := &workerTestStore{heartbeatErr: ErrInvalidTransition}
	registry := NewRegistry()
	registry.Register("wait", func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	worker := &Worker{
		ID:            "worker-a",
		Store:         store,
		Registry:      registry,
		LeaseDuration: 3 * time.Millisecond,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		worker.runJob(context.Background(), Job{ID: "job-1", WorkflowType: "wait", Payload: []byte(`{}`)})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("job was not canceled after lease loss")
	}
	if store.completed {
		t.Fatal("job should not complete after losing its lease")
	}
	if store.failedJobID != "job-1" {
		t.Fatalf("failed job = %q, want job-1", store.failedJobID)
	}
	if !errors.Is(store.failedErr, context.Canceled) {
		t.Fatalf("failure error = %v, want context.Canceled", store.failedErr)
	}
}

func TestWorkerCancelsJobWhenHeartbeatsKeepFailing(t *testing.T) {
	store := &workerTestStore{heartbeatErr: errors.New("db unreachable")}
	registry := NewRegistry()
	registry.Register("wait", func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	worker := &Worker{
		ID:            "worker-a",
		Store:         store,
		Registry:      registry,
		LeaseDuration: 30 * time.Millisecond,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		worker.runJob(context.Background(), Job{ID: "job-1", WorkflowType: "wait", Payload: []byte(`{}`)})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("job was not canceled after sustained heartbeat failures")
	}
	if store.completed {
		t.Fatal("job should not complete without a confirmed lease")
	}
	if !errors.Is(store.failedErr, context.Canceled) {
		t.Fatalf("failure error = %v, want context.Canceled", store.failedErr)
	}
}

func TestWorkerDrainsInFlightJobsOnShutdown(t *testing.T) {
	started := make(chan struct{})
	store := &workerTestStore{
		claimJobs: []Job{{ID: "job-1", WorkflowType: "slow", Payload: []byte(`{}`)}},
	}
	registry := NewRegistry()
	registry.Register("slow", func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		close(started)
		select {
		case <-time.After(25 * time.Millisecond):
			return json.RawMessage(`{"ok":true}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	worker := &Worker{
		ID:              "worker-a",
		Store:           store,
		Registry:        registry,
		PollInterval:    time.Hour,
		LeaseDuration:   time.Second,
		ShutdownTimeout: time.Second,
		Concurrency:     1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()

	<-started
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not drain before timeout")
	}
	if store.completedJobID != "job-1" {
		t.Fatalf("completed job = %q, want job-1", store.completedJobID)
	}
}

func TestWorkerSuspendsJobWithoutFailing(t *testing.T) {
	store := &workerTestStore{}
	registry := NewRegistry()
	registry.Register("wait-a-bit", func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := Sleep(ctx, "settle", time.Hour); err != nil {
			return nil, err
		}
		return json.RawMessage(`{"ok":true}`), nil
	})
	worker := &Worker{
		ID:            "worker-a",
		Store:         store,
		Registry:      registry,
		LeaseDuration: time.Minute,
	}

	worker.runJob(context.Background(), Job{ID: "job-1", WorkflowType: "wait-a-bit", Payload: []byte(`{}`)})

	if store.suspendedJobID != "job-1" {
		t.Fatalf("suspended job = %q, want job-1", store.suspendedJobID)
	}
	if store.completed || store.failedJobID != "" {
		t.Fatal("suspended job must neither complete nor fail")
	}
}

type workerTestStore struct {
	mu sync.Mutex

	claimJobs    []Job
	heartbeatErr error
	steps        map[string]json.RawMessage
	// signals is append-only like the real mailbox: GetSignalAt serves by
	// index, and signalKeys dedupes like the idempotency index.
	signals    map[string][]json.RawMessage
	signalKeys map[string]struct{}
	// getStepMisses simulates a duplicate-execution race where the checkpoint
	// lands after this worker's lookup: GetStep reports not-found while
	// SaveStep still hits the existing row.
	getStepMisses bool

	completed      bool
	completedJobID string
	failedJobID    string
	failedErr      error
	suspendedJobID string
	suspendedAfter time.Time
	jobs           map[string]Job
	events         []Event
	exhausted      []string
	enqueueSeq     int
}

func (s *workerTestStore) Enqueue(_ context.Context, req EnqueueRequest) (Job, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	req = NormalizeEnqueue(req)
	if s.jobs == nil {
		s.jobs = map[string]Job{}
	}
	if req.IdempotencyKey != "" {
		for _, job := range s.jobs {
			if job.IdempotencyKey != nil && *job.IdempotencyKey == req.IdempotencyKey {
				return job, false, nil
			}
		}
	}
	s.enqueueSeq++
	job := Job{
		ID:           "job-enqueued-" + string(rune('a'+s.enqueueSeq)),
		Queue:        req.Queue,
		WorkflowType: req.WorkflowType,
		Payload:      req.Payload,
		Status:       StatusQueued,
		MaxAttempts:  req.MaxAttempts,
		RunAfter:     req.RunAfter,
		Attempt:      0,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	if req.IdempotencyKey != "" {
		key := req.IdempotencyKey
		job.IdempotencyKey = &key
	}
	if req.ParentID != "" {
		parent := req.ParentID
		job.ParentID = &parent
	}
	s.jobs[job.ID] = job
	return job, true, nil
}

func (s *workerTestStore) GetJob(_ context.Context, jobID string) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return Job{}, ErrNotFound
	}
	return job, nil
}

func (s *workerTestStore) setJobStatus(jobID string, status Status) {
	if s.jobs == nil {
		s.jobs = map[string]Job{}
	}
	job := s.jobs[jobID]
	job.Status = status
	s.jobs[jobID] = job
}

func (s *workerTestStore) Claim(context.Context, ClaimOptions) ([]Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	jobs := s.claimJobs
	s.claimJobs = nil
	return jobs, nil
}

func (s *workerTestStore) Heartbeat(context.Context, string, string, time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.heartbeatErr
}

func (s *workerTestStore) Complete(_ context.Context, jobID, _ string, _ []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completed = true
	s.completedJobID = jobID
	return nil
}

func (s *workerTestStore) Fail(_ context.Context, jobID, _ string, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failedJobID = jobID
	s.failedErr = cause
	if _, ok := s.jobs[jobID]; ok {
		s.setJobStatus(jobID, StatusDeadLetter)
	}
	return nil
}

func (s *workerTestStore) Suspend(_ context.Context, jobID, _ string, runAfter time.Time, _ int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.suspendedJobID = jobID
	s.suspendedAfter = runAfter
	return true, nil
}

func (s *workerTestStore) Signal(_ context.Context, jobID, name string, payload []byte, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.signals == nil {
		s.signals = map[string][]json.RawMessage{}
	}
	if s.signalKeys == nil {
		s.signalKeys = map[string]struct{}{}
	}
	if key != "" {
		if _, dup := s.signalKeys[jobID+"/"+name+"/"+key]; dup {
			return nil
		}
		s.signalKeys[jobID+"/"+name+"/"+key] = struct{}{}
	}
	s.signals[jobID+"/"+name] = append(s.signals[jobID+"/"+name], payload)
	return nil
}

func (s *workerTestStore) GetSignalAt(_ context.Context, jobID, name string, index int) (json.RawMessage, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	queue := s.signals[jobID+"/"+name]
	if index < 0 || index >= len(queue) {
		return nil, false, nil
	}
	return queue[index], true, nil
}

func (s *workerTestStore) Cancel(context.Context, string) error {
	return nil
}

func (s *workerTestStore) DeadLetterExhausted(context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exhausted, nil
}

func (s *workerTestStore) RecordEvent(_ context.Context, jobID, eventType string, details []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, Event{JobID: jobID, EventType: eventType, Details: details})
	return nil
}

func (s *workerTestStore) ListSteps(context.Context, string) ([]StepResult, error) {
	return nil, nil
}

func (s *workerTestStore) GetStep(_ context.Context, jobID, stepName string) (json.RawMessage, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getStepMisses {
		return nil, false, nil
	}
	result, ok := s.steps[jobID+"/"+stepName]
	return result, ok, nil
}

func (s *workerTestStore) SaveStep(_ context.Context, jobID, _, stepName string, result json.RawMessage) (json.RawMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.steps[jobID+"/"+stepName]; ok {
		return existing, nil
	}
	if s.steps == nil {
		s.steps = map[string]json.RawMessage{}
	}
	s.steps[jobID+"/"+stepName] = result
	return result, nil
}

func (s *workerTestStore) PruneCompleted(context.Context, string, time.Duration) (int, error) {
	return 0, nil
}

func (s *workerTestStore) RetryDeadLetter(context.Context, string) (Job, error) {
	return Job{}, nil
}

func (s *workerTestStore) Replay(context.Context, string) (Job, error) {
	return Job{}, nil
}

func (s *workerTestStore) Get(_ context.Context, jobID string) (Job, []Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job, ok := s.jobs[jobID]; ok {
		return job, nil, nil
	}
	return Job{}, nil, nil
}

func (s *workerTestStore) List(context.Context, ListOptions) ([]Job, error) {
	return nil, nil
}

func (s *workerTestStore) ListSignals(context.Context, string) ([]Signal, error) {
	return nil, nil
}

func (s *workerTestStore) QueueDepth(context.Context) ([]QueueDepth, error) {
	return nil, nil
}

func (s *workerTestStore) ParkedCount(context.Context) (int64, error) {
	return 0, nil
}
