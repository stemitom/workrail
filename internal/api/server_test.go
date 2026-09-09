package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stemitom/workrail/internal/engine"
)

func TestAuthRequiresBearerToken(t *testing.T) {
	server := New(&fakeStore{}, slog.Default(), Options{AuthToken: "secret"})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	cases := []struct {
		name   string
		path   string
		header string
		want   int
	}{
		{"missing token", "/jobs", "", http.StatusUnauthorized},
		{"wrong token", "/jobs", "Bearer nope", http.StatusUnauthorized},
		{"wrong scheme", "/jobs", "Basic secret", http.StatusUnauthorized},
		{"valid token", "/jobs", "Bearer secret", http.StatusOK},
		{"lowercase scheme", "/jobs", "bearer secret", http.StatusOK},
		{"healthz needs no token", "/healthz", "", http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, ts.URL+tc.path, nil)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			resp, err := ts.Client().Do(req)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.want)
			}
		})
	}
}

func TestNoAuthTokenDisablesAuth(t *testing.T) {
	server := New(&fakeStore{}, slog.Default(), Options{AuthToken: ""})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/jobs")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestSignalEndpoint(t *testing.T) {
	cases := []struct {
		name      string
		body      string
		signalErr error
		want      int
	}{
		{"delivers", `{"name":"approval","payload":{"ok":true}}`, nil, http.StatusAccepted},
		{"missing name", `{"payload":{}}`, nil, http.StatusBadRequest},
		{"bad json", `{`, nil, http.StatusBadRequest},
		{"unknown job", `{"name":"approval"}`, engine.ErrNotFound, http.StatusNotFound},
		{"terminal job", `{"name":"approval"}`, engine.ErrInvalidTransition, http.StatusConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{signalErr: tc.signalErr}
			server := New(store, slog.Default(), Options{AuthToken: ""})
			ts := httptest.NewServer(server.Handler())
			defer ts.Close()

			resp, err := ts.Client().Post(ts.URL+"/jobs/"+fakeJob().ID+"/signals", "application/json", strings.NewReader(tc.body))
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.want)
			}
		})
	}

	store := &fakeStore{}
	server := New(store, slog.Default(), Options{AuthToken: ""})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	resp, err := ts.Client().Post(ts.URL+"/jobs/"+fakeJob().ID+"/signals", "application/json", strings.NewReader(`{"name":"approval"}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()
	if store.signaled.jobID != fakeJob().ID || store.signaled.name != "approval" {
		t.Fatalf("signaled = %+v, want job approval", store.signaled)
	}
}

type fakeStore struct {
	listCount int
	signalErr error
	signaled  struct{ jobID, name string }
	jobs      []engine.Job
	signals   []engine.Signal
	getJob    *engine.Job
}

func TestListSignalsEndpoint(t *testing.T) {
	store := &fakeStore{signals: []engine.Signal{
		{ID: 1, JobID: fakeJob().ID, Name: "approval", Payload: json.RawMessage(`{"ok":true}`)},
	}}
	server := New(store, slog.Default(), Options{AuthToken: ""})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/jobs/" + fakeJob().ID + "/signals")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	body := readBody(t, resp)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, "approval") {
		t.Fatalf("status = %d, body = %.200s", resp.StatusCode, body)
	}
}

func TestListParentFilter(t *testing.T) {
	server := New(&fakeStore{}, slog.Default(), Options{AuthToken: ""})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/jobs?parent_id=not-a-uuid")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}

	resp, err = ts.Client().Get(ts.URL + "/jobs?parent_id=0b81a3a2-9d9a-4a41-b9d3-000000000001")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestUISignalAction(t *testing.T) {
	store := &fakeStore{}
	server := New(store, slog.Default(), Options{AuthToken: ""})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	form := url.Values{"name": {"approval"}, "payload": {`{"ok":true}`}}
	resp, err := ts.Client().PostForm(ts.URL+"/ui/jobs/"+fakeJob().ID+"/signal", form)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()
	if store.signaled.jobID != fakeJob().ID || store.signaled.name != "approval" {
		t.Fatalf("signaled = %+v, want job approval", store.signaled)
	}

	resp, err = ts.Client().PostForm(ts.URL+"/ui/jobs/"+fakeJob().ID+"/signal", url.Values{})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("nameless signal status = %d, want 400", resp.StatusCode)
	}
}

func fakeJob() engine.Job {
	errMsg := "boom"
	return engine.Job{
		ID: "0b81a3a2-9d9a-4a41-b9d3-000000000001", Queue: "emails", WorkflowType: "send_email",
		Status: engine.StatusDeadLetter, Payload: json.RawMessage(`{"user":"u_1"}`),
		Error: &errMsg, Attempt: 3, MaxAttempts: 3,
		CreatedAt: time.Now().Add(-time.Hour), UpdatedAt: time.Now().Add(-time.Minute),
	}
}

func (s *fakeStore) Enqueue(context.Context, engine.EnqueueRequest) (engine.Job, bool, error) {
	return engine.Job{}, false, nil
}

func (s *fakeStore) Claim(context.Context, engine.ClaimOptions) ([]engine.Job, error) {
	return nil, nil
}

func (s *fakeStore) Heartbeat(context.Context, string, string, time.Duration) error {
	return nil
}

func (s *fakeStore) DeadLetterExhausted(context.Context) ([]string, error) {
	return nil, nil
}

func (s *fakeStore) RecordEvent(context.Context, string, string, []byte) error {
	return nil
}

func (s *fakeStore) GetJob(context.Context, string) (engine.Job, error) {
	return engine.Job{}, engine.ErrNotFound
}

func (s *fakeStore) GetStep(context.Context, string, string) (json.RawMessage, bool, error) {
	return nil, false, nil
}

func (s *fakeStore) GetSignalAt(context.Context, string, string, int) (json.RawMessage, bool, error) {
	return nil, false, nil
}

func (s *fakeStore) ListSteps(context.Context, string) ([]engine.StepResult, error) {
	return []engine.StepResult{
		{JobID: fakeJob().ID, Name: "compose", Result: json.RawMessage(`{"ok":true}`), CreatedAt: time.Now().Add(-2 * time.Minute)},
	}, nil
}

func (s *fakeStore) SaveStep(_ context.Context, _, _, _ string, result json.RawMessage) (json.RawMessage, error) {
	return result, nil
}

func (s *fakeStore) PruneCompleted(context.Context, string, time.Duration) (int, error) {
	return 0, nil
}

func (s *fakeStore) Complete(context.Context, string, string, []byte) error {
	return nil
}

func (s *fakeStore) Fail(context.Context, string, string, error) error {
	return nil
}

func (s *fakeStore) Suspend(context.Context, string, string, time.Time, int) (bool, error) {
	return true, nil
}

func (s *fakeStore) Signal(_ context.Context, jobID, name string, _ []byte, _ string) error {
	s.signaled.jobID, s.signaled.name = jobID, name
	return s.signalErr
}

func (s *fakeStore) Cancel(context.Context, string) error {
	return nil
}

func (s *fakeStore) RetryDeadLetter(context.Context, string) (engine.Job, error) {
	return engine.Job{}, nil
}

func (s *fakeStore) Replay(context.Context, string) (engine.Job, error) {
	return engine.Job{}, nil
}

func (s *fakeStore) Get(context.Context, string) (engine.Job, []engine.Event, error) {
	events := []engine.Event{
		{ID: 1, JobID: fakeJob().ID, EventType: "job.enqueued", Details: json.RawMessage(`{}`), CreatedAt: time.Now().Add(-time.Hour)},
	}
	if s.getJob != nil {
		return *s.getJob, events, nil
	}
	return fakeJob(), events, nil
}

func (s *fakeStore) List(_ context.Context, opts engine.ListOptions) ([]engine.Job, error) {
	jobs := s.jobs
	if jobs == nil {
		jobs = []engine.Job{fakeJob()}
		for i := 1; i < s.listCount; i++ {
			job := fakeJob()
			job.ID = fmt.Sprintf("0b81a3a2-9d9a-4a41-b9d3-%012d", i)
			jobs = append(jobs, job)
		}
	}
	if opts.ParentID != "" {
		var children []engine.Job
		for _, job := range jobs {
			if job.ParentID != nil && *job.ParentID == opts.ParentID {
				children = append(children, job)
			}
		}
		return children, nil
	}
	return jobs, nil
}

func (s *fakeStore) ListSignals(context.Context, string) ([]engine.Signal, error) {
	return s.signals, nil
}

func (s *fakeStore) QueueDepth(context.Context) ([]engine.QueueDepth, error) {
	return []engine.QueueDepth{
		{Queue: "emails", Status: string(engine.StatusQueued), Count: 4},
		{Queue: "emails", Status: string(engine.StatusRunning), Count: 2},
		{Queue: "emails", Status: string(engine.StatusDeadLetter), Count: 1},
		{Queue: "billing", Status: string(engine.StatusSucceeded), Count: 12},
	}, nil
}
