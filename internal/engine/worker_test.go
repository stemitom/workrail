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

type workerTestStore struct {
	mu sync.Mutex

	claimJobs    []Job
	heartbeatErr error

	completed      bool
	completedJobID string
	failedJobID    string
	failedErr      error
}

func (s *workerTestStore) Enqueue(context.Context, EnqueueRequest) (Job, bool, error) {
	return Job{}, false, nil
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
	return nil
}

func (s *workerTestStore) Cancel(context.Context, string) error {
	return nil
}

func (s *workerTestStore) DeadLetterExhausted(context.Context) (int, error) {
	return 0, nil
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

func (s *workerTestStore) Get(context.Context, string) (Job, []Event, error) {
	return Job{}, nil, nil
}

func (s *workerTestStore) List(context.Context, ListOptions) ([]Job, error) {
	return nil, nil
}

func (s *workerTestStore) QueueDepth(context.Context) ([]QueueDepth, error) {
	return nil, nil
}
