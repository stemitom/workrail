package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stemitom/workrail/internal/engine"
	"github.com/stemitom/workrail/internal/redact"
)

func TestDashboardRendersPages(t *testing.T) {
	server := New(&fakeStore{}, slog.Default(), Options{AuthToken: ""})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	cases := []struct {
		path string
		want []string
	}{
		{"/ui", []string{"Overview", "emails", "dead letter", "send_email"}},
		{"/ui/jobs", []string{"Jobs", "send_email", "0b81a3a2"}},
		{"/ui/jobs?status=dead_letter", []string{"Dead letters"}},
		{"/ui/jobs/" + fakeJob().ID, []string{"send_email", "compose", "job.enqueued", "boom", "Retry"}},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			resp, err := ts.Client().Get(ts.URL + tc.path)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			body := readBody(t, resp)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body: %.200s", resp.StatusCode, body)
			}
			for _, want := range tc.want {
				if !strings.Contains(body, want) {
					t.Fatalf("page %s missing %q", tc.path, want)
				}
			}
		})
	}
}

func TestDashboardAuthFlow(t *testing.T) {
	server := New(&fakeStore{}, slog.Default(), Options{AuthToken: "secret"})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}

	resp, err := client.Get(ts.URL + "/ui")
	if err != nil {
		t.Fatalf("get without session: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/ui/login" {
		t.Fatalf("unauthenticated /ui: status=%d location=%q, want redirect to /ui/login", resp.StatusCode, resp.Header.Get("Location"))
	}

	resp, err = client.PostForm(ts.URL+"/ui/login", url.Values{"token": {"wrong"}})
	if err != nil {
		t.Fatalf("bad login: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad login status = %d, want 401", resp.StatusCode)
	}

	resp, err = client.PostForm(ts.URL+"/ui/login", url.Values{"token": {"secret"}})
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	resp.Body.Close()
	var session *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == sessionCookie {
			session = c
		}
	}
	if session == nil || !session.HttpOnly {
		t.Fatalf("login must set an HttpOnly session cookie, got %+v", resp.Cookies())
	}
	if session.Value == "secret" {
		t.Fatal("session cookie must not store the raw API token")
	}

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/ui", nil)
	req.AddCookie(session)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("get with session: %v", err)
	}
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, "Overview") {
		t.Fatalf("authenticated /ui: status=%d", resp.StatusCode)
	}

	// The cookie must not satisfy API bearer auth paths, and vice versa.
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/jobs", nil)
	req.AddCookie(session)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("api with cookie: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("cookie on API path: status = %d, want 401", resp.StatusCode)
	}
}

func TestJobsPagination(t *testing.T) {
	server := New(&fakeStore{listCount: uiJobsLimit}, slog.Default(), Options{AuthToken: ""})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/ui/jobs?queue=emails")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body := readBody(t, resp)
	if !strings.Contains(body, "Older") || !strings.Contains(body, "before=") || !strings.Contains(body, "queue=emails") {
		t.Fatalf("full page must offer an Older link preserving filters")
	}

	resp, err = ts.Client().Get(ts.URL + "/ui/jobs?before=2026-07-01T00%3A00%3A00Z&before_id=abc")
	if err != nil {
		t.Fatalf("get older page: %v", err)
	}
	body = readBody(t, resp)
	if !strings.Contains(body, "Latest") {
		t.Fatal("cursored page must offer a Latest link")
	}

	resp, err = ts.Client().Get(ts.URL + "/ui/jobs?before=not-a-time")
	if err != nil {
		t.Fatalf("get bad cursor: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad cursor status = %d, want 400", resp.StatusCode)
	}
}

func TestLoginRateLimit(t *testing.T) {
	server := New(&fakeStore{}, slog.Default(), Options{AuthToken: "secret"})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	var last int
	for range loginFailureLimit + 1 {
		resp, err := ts.Client().PostForm(ts.URL+"/ui/login", url.Values{"token": {"wrong"}})
		if err != nil {
			t.Fatalf("login attempt: %v", err)
		}
		resp.Body.Close()
		last = resp.StatusCode
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("attempt %d status = %d, want 429", loginFailureLimit+1, last)
	}
}

func TestDashboardPostRejectsCrossOrigin(t *testing.T) {
	server := New(&fakeStore{}, slog.Default(), Options{AuthToken: "secret"})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/ui/jobs/abc/cancel", nil)
	req.Header.Set("Origin", "https://evil.example")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin POST status = %d, want 403", resp.StatusCode)
	}
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(body)
}

func TestDashboardRedactsConfiguredFields(t *testing.T) {
	server := New(&fakeStore{}, slog.Default(), Options{RedactFields: []string{"user"}})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/ui/jobs/" + fakeJob().ID)
	if err != nil {
		t.Fatalf("get job page: %v", err)
	}
	body := readBody(t, resp)
	if strings.Contains(body, "u_1") {
		t.Fatal("job page rendered a redacted field's value")
	}
	if !strings.Contains(body, redact.Mask) {
		t.Fatalf("job page missing %s marker", redact.Mask)
	}

	// The JSON API authenticates separately and machine callers need the real
	// payload, so redaction must not leak into it.
	resp, err = ts.Client().Get(ts.URL + "/jobs/" + fakeJob().ID)
	if err != nil {
		t.Fatalf("get job JSON: %v", err)
	}
	if apiBody := readBody(t, resp); !strings.Contains(apiBody, "u_1") {
		t.Fatalf("JSON API should not redact, got %.200s", apiBody)
	}
}

func TestDashboardJobShowsLineageAndMailbox(t *testing.T) {
	parentID := fakeJob().ID
	childID := "0b81a3a2-9d9a-4a41-b9d3-000000000042"
	child := fakeJob()
	child.ID = childID
	child.WorkflowType = "settlement"
	child.ParentID = &parentID
	child.CreatedAt = time.Now().Add(-10 * time.Minute)
	parent := fakeJob()
	parent.RunAfter = time.Now().Add(time.Hour)
	parent.Status = engine.StatusQueued
	store := &fakeStore{
		getJob:  &parent,
		jobs:    []engine.Job{parent, child},
		signals: []engine.Signal{{ID: 1, JobID: parentID, Name: "approval", CreatedAt: time.Now().Add(-5 * time.Minute)}},
	}
	server := New(store, slog.Default(), Options{AuthToken: ""})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/ui/jobs/" + parentID)
	if err != nil {
		t.Fatalf("get job page: %v", err)
	}
	body := readBody(t, resp)
	for _, want := range []string{"Parked until", "Execution path", "Step compose", "settlement", "Signal approval", "Send signal"} {
		if !strings.Contains(body, want) {
			t.Fatalf("job page missing %q", want)
		}
	}
	// Steps, signals, and children merged chronologically: the child predates
	// the signal, which predates the step checkpoint.
	childAt := strings.Index(body, "Child settlement")
	signalAt := strings.Index(body, "Signal approval")
	composeAt := strings.Index(body, "Step compose")
	if !(childAt < signalAt && signalAt < composeAt) {
		t.Fatal("execution path is not chronological")
	}
}

func TestDashboardOverviewLeadsWithAttention(t *testing.T) {
	store := &fakeStore{parked: 3}
	server := New(store, slog.Default(), Options{AuthToken: ""})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/ui")
	if err != nil {
		t.Fatalf("get overview: %v", err)
	}
	body := readBody(t, resp)
	for _, want := range []string{"Needs attention", "parked", ">3<", "All dead letters"} {
		if !strings.Contains(body, want) {
			t.Fatalf("overview missing %q", want)
		}
	}
	// Dead-letter tile leads the execution tiles.
	dl := strings.Index(body, ">dead letter</div>")
	running := strings.Index(body, ">running</div>")
	if dl < 0 || running < 0 || dl > running {
		t.Fatal("dead-letter tile should lead the tiles")
	}
}
