package api

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stemitom/workrail/internal/engine"
)

func TestAuthRequiresBearerToken(t *testing.T) {
	server := New(&fakeStore{}, slog.Default(), "secret")
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
	server := New(&fakeStore{}, slog.Default(), "")
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

type fakeStore struct{}

func (s *fakeStore) Enqueue(context.Context, engine.EnqueueRequest) (engine.Job, bool, error) {
	return engine.Job{}, false, nil
}

func (s *fakeStore) Claim(context.Context, engine.ClaimOptions) ([]engine.Job, error) {
	return nil, nil
}

func (s *fakeStore) Heartbeat(context.Context, string, string, time.Duration) error {
	return nil
}

func (s *fakeStore) DeadLetterExhausted(context.Context) (int, error) {
	return 0, nil
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
	return engine.Job{}, nil, nil
}

func (s *fakeStore) List(context.Context, engine.ListOptions) ([]engine.Job, error) {
	return nil, nil
}

func (s *fakeStore) QueueDepth(context.Context) ([]engine.QueueDepth, error) {
	return nil, nil
}
