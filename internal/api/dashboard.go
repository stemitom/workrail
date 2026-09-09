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
	"maps"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/stemitom/workrail/internal/engine"
	"github.com/stemitom/workrail/internal/redact"
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

// templateFuncs binds the JSON renderers to a redactor so every payload,
// result, and event detail the dashboard prints goes through it. The JSON API
// is unaffected: it authenticates with the bearer token rather than the
// dashboard session, so masking there would hide data from the machine callers
// that need it without protecting anything a session holder can already reach.
func templateFuncs(redactor *redact.Redactor) template.FuncMap {
	return template.FuncMap{
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
		"prettyJSON": func(data json.RawMessage) string {
			return prettyJSON(redactor.JSON(data))
		},
		"compactJSON": func(data json.RawMessage) string {
			return compactJSON(redactor.JSON(data))
		},
		"eventClass": eventClass,
		"isParked":   isParked,
		"isPermanent": func(details json.RawMessage) bool {
			var parsed struct {
				Permanent bool `json:"permanent"`
			}
			return json.Unmarshal(details, &parsed) == nil && parsed.Permanent
		},
	}
}

func parseTemplates(redactor *redact.Redactor) map[string]*template.Template {
	funcs := templateFuncs(redactor)
	pages := map[string]*template.Template{}
	for _, page := range []string{"overview", "jobs", "job", "login", "error"} {
		pages[page] = template.Must(template.New("layout.gohtml").Funcs(funcs).
			ParseFS(templateFS, "templates/layout.gohtml", "templates/"+page+".gohtml"))
	}
	return pages
}

type view struct {
	Title       string
	Page        string
	Chromeless  bool
	AuthEnabled bool
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
		Title: "Overview", Page: "overview",
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
	Jobs      []engine.Job
	Queue     string
	Status    engine.Status
	Type      string
	Statuses  []engine.Status
	OlderURL  string
	LatestURL string
}

const uiJobsLimit = 100

func (s *Server) uiJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	status := engine.Status(q.Get("status"))
	if !engine.IsValidStatus(status) {
		s.uiStoreError(w, engine.ErrInvalidStatus)
		return
	}
	before, beforeID, err := parseCursor(q)
	if err != nil {
		s.render(w, "error", http.StatusBadRequest, view{Title: "Error", Data: "That page cursor isn't valid."})
		return
	}
	jobs, err := s.store.List(r.Context(), engine.ListOptions{
		Limit:           uiJobsLimit,
		Queue:           q.Get("queue"),
		Status:          status,
		WorkflowType:    q.Get("type"),
		BeforeCreatedAt: before,
		BeforeID:        beforeID,
	})
	if err != nil {
		s.uiStoreError(w, err)
		return
	}
	page := "jobs"
	if status == engine.StatusDeadLetter {
		page = "dlq"
	}
	filters := url.Values{}
	for _, key := range []string{"queue", "status", "type"} {
		if q.Get(key) != "" {
			filters.Set(key, q.Get(key))
		}
	}
	data := jobsData{Jobs: jobs, Queue: q.Get("queue"), Status: status, Type: q.Get("type"), Statuses: dashboardStatuses}
	if len(jobs) == uiJobsLimit {
		last := jobs[len(jobs)-1]
		older := url.Values{}
		maps.Copy(older, filters)
		older.Set("before", last.CreatedAt.UTC().Format(time.RFC3339Nano))
		older.Set("before_id", last.ID)
		data.OlderURL = "/ui/jobs?" + older.Encode()
	}
	if !before.IsZero() {
		data.LatestURL = "/ui/jobs"
		if len(filters) > 0 {
			data.LatestURL += "?" + filters.Encode()
		}
	}
	s.render(w, "jobs", http.StatusOK, view{Title: "Jobs", Page: page, Data: data})
}

type jobData struct {
	Job       engine.Job
	Events    []engine.Event
	Steps     []engine.StepResult
	Signals   []engine.Signal
	Children  []engine.Job
	Parent    *engine.Job
	CanRetry  bool
	CanCancel bool
}

// isParked reports whether the job is waiting out a timer, signal nap, or
// delayed start instead of being ready to run.
func isParked(job engine.Job) bool {
	switch job.Status {
	case engine.StatusQueued, engine.StatusRetrying:
		return job.RunAfter.After(time.Now())
	default:
		return false
	}
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
	signals, err := s.store.ListSignals(r.Context(), job.ID)
	if err != nil {
		s.uiStoreError(w, err)
		return
	}
	children, err := s.store.List(r.Context(), engine.ListOptions{Limit: uiJobsLimit, ParentID: job.ID})
	if err != nil {
		s.uiStoreError(w, err)
		return
	}
	data := jobData{
		Job:       job,
		Events:    events,
		Steps:     steps,
		Signals:   signals,
		Children:  children,
		CanRetry:  job.Status == engine.StatusDeadLetter,
		CanCancel: job.Status == engine.StatusQueued || job.Status == engine.StatusRunning || job.Status == engine.StatusRetrying,
	}
	if job.ParentID != nil {
		parent, _, err := s.store.Get(r.Context(), *job.ParentID)
		if err != nil && !errors.Is(err, engine.ErrNotFound) {
			s.uiStoreError(w, err)
			return
		}
		if err == nil {
			data.Parent = &parent
		}
	}
	s.render(w, "job", http.StatusOK, view{
		Title: job.WorkflowType, Page: "jobs",
		Data: data,
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

func (s *Server) uiSignal(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := r.ParseForm(); err != nil {
		s.render(w, "error", http.StatusBadRequest, view{Title: "Error", Data: "That form didn't parse."})
		return
	}
	name := r.FormValue("name")
	payload := r.FormValue("payload")
	if payload == "" {
		payload = "{}"
	}
	if name == "" {
		s.render(w, "error", http.StatusBadRequest, view{Title: "Error", Data: "A signal needs a name."})
		return
	}
	if !json.Valid([]byte(payload)) {
		s.render(w, "error", http.StatusBadRequest, view{Title: "Error", Data: "That payload isn't valid JSON."})
		return
	}
	if err := s.store.Signal(r.Context(), id, name, json.RawMessage(payload), ""); err != nil {
		s.uiStoreError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/jobs/"+id, http.StatusSeeOther)
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

// eventClass colors history rows that mark workflow-engine outcomes. Unknown
// types render as plain text (the pill defaults neutral).
func eventClass(eventType string) string {
	switch eventType {
	case "job.succeeded", "job.compensated":
		return "st-succeeded"
	case "job.failed", "job.compensation_failed":
		return "st-failed"
	case "job.dead_lettered":
		return "st-dead_letter"
	case "job.canceled":
		return "st-canceled"
	case "job.signaled":
		return "st-running"
	case "job.suspended":
		return "st-retrying"
	default:
		return ""
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
