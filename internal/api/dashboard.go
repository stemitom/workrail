package api

import (
	"bytes"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/stemitom/workrail/internal/engine"
)

//go:embed templates
var templateFS embed.FS

const sessionCookie = "workrail_session"

var dashboardStatuses = []engine.Status{
	engine.StatusQueued, engine.StatusRunning, engine.StatusRetrying,
	engine.StatusSucceeded, engine.StatusDeadLetter, engine.StatusCanceled,
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
	"rfc3339":     func(t time.Time) string { return t.UTC().Format(time.RFC3339) },
	"prettyJSON":  prettyJSON,
	"compactJSON": compactJSON,
}

func parseTemplates() map[string]*template.Template {
	pages := map[string]*template.Template{}
	for _, page := range []string{"overview", "jobs", "job", "login"} {
		pages[page] = template.Must(template.New("layout.gohtml").Funcs(templateFuncs).
			ParseFS(templateFS, "templates/layout.gohtml", "templates/"+page+".gohtml"))
	}
	return pages
}

type view struct {
	Title       string
	Page        string
	ShowChrome  bool
	AuthEnabled bool
	Refresh     bool
	Data        any
}

func (s *Server) render(w http.ResponseWriter, page string, v view) {
	v.AuthEnabled = len(s.authToken) > 0
	var buf bytes.Buffer
	if err := s.templates[page].Execute(&buf, v); err != nil {
		s.logger.Error("render dashboard page failed", "page", page, "error", err)
		http.Error(w, "template error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
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
	s.render(w, "overview", view{
		Title: "Overview", Page: "overview", ShowChrome: true, Refresh: true,
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
		http.Error(w, "invalid status", http.StatusBadRequest)
		return
	}
	jobs, err := s.store.List(r.Context(), engine.ListOptions{
		Limit:        100,
		Queue:        q.Get("queue"),
		Status:       status,
		WorkflowType: q.Get("type"),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	page := "jobs"
	if status == engine.StatusDeadLetter {
		page = "dlq"
	}
	s.render(w, "jobs", view{
		Title: "Jobs", Page: page, ShowChrome: true, Refresh: true,
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
		writeStoreError(w, err)
		return
	}
	steps, err := s.store.ListSteps(r.Context(), job.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.render(w, "job", view{
		Title: job.WorkflowType, Page: "jobs", ShowChrome: true,
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
		writeStoreError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/jobs/"+id, http.StatusSeeOther)
}

func (s *Server) uiCancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.store.Cancel(r.Context(), id); err != nil {
		writeStoreError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/jobs/"+id, http.StatusSeeOther)
}

func (s *Server) uiReplay(w http.ResponseWriter, r *http.Request) {
	job, err := s.store.Replay(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/jobs/"+job.ID, http.StatusSeeOther)
}

func (s *Server) uiLoginForm(w http.ResponseWriter, r *http.Request) {
	if len(s.authToken) == 0 {
		http.Redirect(w, r, "/ui", http.StatusSeeOther)
		return
	}
	s.render(w, "login", view{Title: "Sign in", Page: "login"})
}

func (s *Server) uiLogin(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	token := r.PostFormValue("token")
	if subtle.ConstantTimeCompare([]byte(token), s.authToken) != 1 {
		w.WriteHeader(http.StatusUnauthorized)
		s.render(w, "login", view{Title: "Sign in", Page: "login", Data: "That token didn't match."})
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: token, Path: "/",
		HttpOnly: true, SameSite: http.SameSiteStrictMode,
	})
	http.Redirect(w, r, "/ui", http.StatusSeeOther)
}

func (s *Server) uiLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: "", Path: "/", MaxAge: -1,
		HttpOnly: true, SameSite: http.SameSiteStrictMode,
	})
	http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
}

func (s *Server) hasSession(r *http.Request) bool {
	cookie, err := r.Cookie(sessionCookie)
	return err == nil && subtle.ConstantTimeCompare([]byte(cookie.Value), s.authToken) == 1
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
