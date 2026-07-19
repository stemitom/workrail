package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestDashboardRendersPages(t *testing.T) {
	server := New(&fakeStore{}, slog.Default(), "")
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
	server := New(&fakeStore{}, slog.Default(), "secret")
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

func TestLoginRateLimit(t *testing.T) {
	server := New(&fakeStore{}, slog.Default(), "secret")
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
	server := New(&fakeStore{}, slog.Default(), "secret")
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
