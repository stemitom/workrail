package api

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/stemitom/workrail/internal/engine"
)

//go:embed templates
var templateFS embed.FS

const sessionCookie = "workrail_session"

// sessionValue derives the cookie value from the auth token with HMAC, so the
// cookie never carries the API credential itself: a leaked cookie grants
// dashboard access until the token rotates, but cannot be replayed as a
// bearer token, and the hex digest is always cookie-safe.
func sessionValue(authToken []byte) string {
	mac := hmac.New(sha256.New, authToken)
	mac.Write([]byte("workrail-session-v1"))
	return hex.EncodeToString(mac.Sum(nil))
}

// loginLimiter bounds failed sign-in attempts so the exempt /ui/login POST is
// not an unthrottled oracle for brute-forcing the token.
type loginLimiter struct {
	mu       sync.Mutex
	failures int
	windowAt time.Time
}

const (
	loginFailureLimit  = 10
	loginFailureWindow = time.Minute
)

func (l *loginLimiter) allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if time.Since(l.windowAt) > loginFailureWindow {
		l.failures = 0
		l.windowAt = time.Now()
	}
	return l.failures < loginFailureLimit
}

func (l *loginLimiter) recordFailure() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if time.Since(l.windowAt) > loginFailureWindow {
		l.failures = 0
		l.windowAt = time.Now()
	}
	l.failures++
}

// requestIsSecure reports whether the client connection used TLS, directly or
// via a reverse proxy that sets X-Forwarded-Proto.
func requestIsSecure(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

var dashboardStatuses = []engine.Status{
	engine.StatusQueued, engine.StatusRunning, engine.StatusRetrying,
	engine.StatusSucceeded, engine.StatusFailed, engine.StatusDeadLetter, engine.StatusCanceled,
}

// liveStatuses are the states drawn as depth-bar segments; terminal bulk
// states (succeeded, canceled) would dwarf the backlog the bar exists to show.
var liveStatuses = []engine.Status{
	engine.StatusQueued, engine.StatusRunning, engine.StatusRetrying, engine.StatusDeadLetter,
}

var templateFuncs = template.FuncMap{
	"shortID":     shortID,
	"statusClass": statusClass,
	"statusLabel": statusLabel,
	"timeago":     timeago,
	"rfc3339": func(t time.Time) string {
		if t.IsZero() {
			return "—"
		}
		return t.UTC().Format(time.RFC3339)
	},
	"deref": func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	},
	"prettyJSON":  prettyJSON,
	"compactJSON": compactJSON,
}

func parseTemplates() map[string]*template.Template {
	pages := map[string]*template.Template{}
	for _, page := range []string{"overview", "jobs", "job", "login", "error"} {
		pages[page] = template.Must(template.New("layout.gohtml").Funcs(templateFuncs).
			ParseFS(templateFS, "templates/layout.gohtml", "templates/"+page+".gohtml"))
	}
	return pages
}

type view struct {
	Title       string
	Page        string
	Chromeless  bool
	AuthEnabled bool
	Refresh     bool
	Data        any
}

func (s *Server) render(w http.ResponseWriter, page string, status int, v view) {
	v.AuthEnabled = len(s.authToken) > 0
	var buf bytes.Buffer
	if err := s.templates[page].Execute(&buf, v); err != nil {
		s.logger.Error("render dashboard page failed", "page", page, "error", err)
		http.Error(w, "template error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = buf.WriteTo(w)
}

func (s *Server) uiStoreError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	message := "Something went wrong. Check the server logs."
	switch {
	case errors.Is(err, engine.ErrNotFound):
		status, message = http.StatusNotFound, "That job doesn't exist."
	case errors.Is(err, engine.ErrInvalidTransition):
		status, message = http.StatusConflict, "The job changed state underneath this action. Go back and refresh."
	case errors.Is(err, engine.ErrInvalidStatus):
		status, message = http.StatusBadRequest, "That status filter isn't valid."
	}
	s.render(w, "error", status, view{Title: "Error", Page: "", Data: message})
}

type overviewData struct {
	Tiles  []depthSegment
	Queues []queueDepthView
	Jobs   []engine.Job
}

type queueDepthView struct {
	Name     string
	Total    int64
	Segments []depthSegment
	Counts   []depthSegment
}

type depthSegment struct {
	Label string
	Class string
	Count int64
}

func (s *Server) uiOverview(w http.ResponseWriter, r *http.Request) {
	depths, err := s.store.QueueDepth(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jobs, err := s.store.List(r.Context(), engine.ListOptions{Limit: 12})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.render(w, "overview", http.StatusOK, view{
		Title: "Overview", Page: "overview", Refresh: true,
		Data: buildOverview(depths, jobs),
	})
}

func buildOverview(depths []engine.QueueDepth, jobs []engine.Job) overviewData {
	totals := map[engine.Status]int64{}
	byQueue := map[string]map[engine.Status]int64{}
	var queueOrder []string
	for _, d := range depths {
		status := engine.Status(d.Status)
		totals[status] += d.Count
		if _, ok := byQueue[d.Queue]; !ok {
			byQueue[d.Queue] = map[engine.Status]int64{}
			queueOrder = append(queueOrder, d.Queue)
		}
		byQueue[d.Queue][status] += d.Count
	}

	data := overviewData{Jobs: jobs}
	for _, status := range []engine.Status{engine.StatusQueued, engine.StatusRunning, engine.StatusDeadLetter, engine.StatusSucceeded} {
		data.Tiles = append(data.Tiles, depthSegment{Label: statusLabel(status), Class: statusClass(status), Count: totals[status]})
	}
	for _, queue := range queueOrder {
		counts := byQueue[queue]
		qv := queueDepthView{Name: queue}
		for _, status := range dashboardStatuses {
			count := counts[status]
			qv.Total += count
			if count > 0 {
				qv.Counts = append(qv.Counts, depthSegment{Label: statusLabel(status), Class: statusClass(status), Count: count})
			}
		}
		for _, status := range liveStatuses {
			if count := counts[status]; count > 0 {
				qv.Segments = append(qv.Segments, depthSegment{Label: statusLabel(status), Class: statusClass(status), Count: count})
			}
		}
		data.Queues = append(data.Queues, qv)
	}
	return data
}

type jobsData struct {
	Jobs     []engine.Job
	Queue    string
	Status   engine.Status
	Type     string
	Statuses []engine.Status
}

func (s *Server) uiJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	status := engine.Status(q.Get("status"))
	if !engine.IsValidStatus(status) {
		s.uiStoreError(w, engine.ErrInvalidStatus)
		return
	}
	jobs, err := s.store.List(r.Context(), engine.ListOptions{
		Limit:        100,
		Queue:        q.Get("queue"),
		Status:       status,
		WorkflowType: q.Get("type"),
	})
	if err != nil {
		s.uiStoreError(w, err)
		return
	}
	page := "jobs"
	if status == engine.StatusDeadLetter {
		page = "dlq"
	}
	s.render(w, "jobs", http.StatusOK, view{
		Title: "Jobs", Page: page,
		Data: jobsData{Jobs: jobs, Queue: q.Get("queue"), Status: status, Type: q.Get("type"), Statuses: dashboardStatuses},
	})
}

type jobData struct {
	Job       engine.Job
	Events    []engine.Event
	Steps     []engine.StepResult
	CanRetry  bool
	CanCancel bool
}

func (s *Server) uiJob(w http.ResponseWriter, r *http.Request) {
	job, events, err := s.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.uiStoreError(w, err)
		return
	}
	steps, err := s.store.ListSteps(r.Context(), job.ID)
	if err != nil {
		s.uiStoreError(w, err)
		return
	}
	s.render(w, "job", http.StatusOK, view{
		Title: job.WorkflowType, Page: "jobs",
		Data: jobData{
			Job:       job,
			Events:    events,
			Steps:     steps,
			CanRetry:  job.Status == engine.StatusDeadLetter,
			CanCancel: job.Status == engine.StatusQueued || job.Status == engine.StatusRunning || job.Status == engine.StatusRetrying,
		},
	})
}

func (s *Server) uiRetry(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.store.RetryDeadLetter(r.Context(), id); err != nil {
		s.uiStoreError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/jobs/"+id, http.StatusSeeOther)
}

func (s *Server) uiCancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.store.Cancel(r.Context(), id); err != nil {
		s.uiStoreError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/jobs/"+id, http.StatusSeeOther)
}

func (s *Server) uiReplay(w http.ResponseWriter, r *http.Request) {
	job, err := s.store.Replay(r.Context(), r.PathValue("id"))
	if err != nil {
		s.uiStoreError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/jobs/"+job.ID, http.StatusSeeOther)
}

func (s *Server) uiLoginForm(w http.ResponseWriter, r *http.Request) {
	if len(s.authToken) == 0 {
		http.Redirect(w, r, "/ui", http.StatusSeeOther)
		return
	}
	s.render(w, "login", http.StatusOK, view{Title: "Sign in", Page: "login", Chromeless: true})
}

func (s *Server) uiLogin(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !s.loginLimits.allow() {
		s.render(w, "login", http.StatusTooManyRequests, view{
			Title: "Sign in", Page: "login", Chromeless: true,
			Data: "Too many attempts. Try again in a minute.",
		})
		return
	}
	token := r.PostFormValue("token")
	if subtle.ConstantTimeCompare([]byte(token), s.authToken) != 1 {
		s.loginLimits.recordFailure()
		s.render(w, "login", http.StatusUnauthorized, view{
			Title: "Sign in", Page: "login", Chromeless: true,
			Data: "That token didn't match.",
		})
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: sessionValue(s.authToken), Path: "/",
		HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: requestIsSecure(r),
	})
	http.Redirect(w, r, "/ui", http.StatusSeeOther)
}

func (s *Server) uiLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: "", Path: "/", MaxAge: -1,
		HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: requestIsSecure(r),
	})
	http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
}

func (s *Server) hasSession(r *http.Request) bool {
	cookie, err := r.Cookie(sessionCookie)
	return err == nil && subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(sessionValue(s.authToken))) == 1
}

func shortID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

func statusClass(status engine.Status) string {
	return "st-" + string(status)
}

func statusLabel(status engine.Status) string {
	switch status {
	case engine.StatusDeadLetter:
		return "dead letter"
	default:
		return string(status)
	}
}

func timeago(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	d := time.Since(t)
	switch {
	case d < 0:
		return "in " + timespan(-d)
	case d < 5*time.Second:
		return "just now"
	default:
		return timespan(d) + " ago"
	}
}

func timespan(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

func prettyJSON(data json.RawMessage) string {
	var buf bytes.Buffer
	if err := json.Indent(&buf, data, "", "  "); err != nil {
		return string(data)
	}
	return buf.String()
}

func compactJSON(data json.RawMessage) string {
	var buf bytes.Buffer
	if err := json.Compact(&buf, data); err != nil {
		return string(data)
	}
	return buf.String()
}
