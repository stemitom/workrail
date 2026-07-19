package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"html/template"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/stemitom/workrail/internal/engine"
	"github.com/stemitom/workrail/internal/observability"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type Server struct {
	store     engine.Store
	logger    *slog.Logger
	mux       *http.ServeMux
	authToken []byte
	templates map[string]*template.Template
}

func New(store engine.Store, logger *slog.Logger, authToken string) *Server {
	s := &Server{
		store: store, logger: logger, mux: http.NewServeMux(),
		authToken: []byte(authToken), templates: parseTemplates(),
	}
	observability.RegisterQueueDepthCollector(store)
	s.routes()
	if authToken == "" {
		logger.Warn("api auth token not configured; all endpoints are unauthenticated")
	}
	return s
}

// Handler authenticates before tracing and metrics so unauthenticated probes
// of arbitrary paths cannot create unbounded metric label cardinality.
func (s *Server) Handler() http.Handler {
	return s.auth(otelhttp.NewHandler(observability.HTTPMetrics(s.mux), "workrail.http"))
}

// auth exempts /healthz so load balancers can probe without credentials, and
// the login page so browsers can reach it. Dashboard paths authenticate with
// the session cookie; everything else requires the bearer token.
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(s.authToken) == 0 || r.URL.Path == "/healthz" || r.URL.Path == "/ui/login" {
			next.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/" || r.URL.Path == "/ui" || strings.HasPrefix(r.URL.Path, "/ui/") {
			if s.hasSession(r) {
				next.ServeHTTP(w, r)
				return
			}
			http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
			return
		}
		scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || subtle.ConstantTimeCompare([]byte(token), s.authToken) != 1 {
			writeError(w, http.StatusUnauthorized, errors.New("missing or invalid bearer token"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /jobs", s.enqueue)
	s.mux.HandleFunc("GET /jobs", s.list)
	s.mux.HandleFunc("GET /dlq", s.deadLetters)
	s.mux.HandleFunc("GET /jobs/{id}", s.inspect)
	s.mux.HandleFunc("POST /jobs/{id}/cancel", s.cancel)
	s.mux.HandleFunc("POST /jobs/{id}/replay", s.replay)
	s.mux.HandleFunc("POST /jobs/{id}/retry", s.retryDeadLetter)
	s.mux.Handle("GET /metrics", promhttp.Handler())
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	s.mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui", http.StatusSeeOther)
	})
	s.mux.HandleFunc("GET /ui", s.uiOverview)
	s.mux.HandleFunc("GET /ui/jobs", s.uiJobs)
	s.mux.HandleFunc("GET /ui/jobs/{id}", s.uiJob)
	s.mux.HandleFunc("POST /ui/jobs/{id}/retry", s.uiRetry)
	s.mux.HandleFunc("POST /ui/jobs/{id}/cancel", s.uiCancel)
	s.mux.HandleFunc("POST /ui/jobs/{id}/replay", s.uiReplay)
	s.mux.HandleFunc("GET /ui/login", s.uiLoginForm)
	s.mux.HandleFunc("POST /ui/login", s.uiLogin)
	s.mux.HandleFunc("POST /ui/logout", s.uiLogout)
}

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	var req engine.EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	job, inserted, err := s.store.Enqueue(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	status := http.StatusCreated
	if !inserted {
		status = http.StatusOK
	}
	observability.JobsEnqueued.WithLabelValues(job.Queue, job.WorkflowType).Inc()
	writeJSON(w, status, job)
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	jobs, err := s.store.List(r.Context(), engine.ListOptions{
		Limit:        limit,
		Queue:        r.URL.Query().Get("queue"),
		Status:       engine.Status(r.URL.Query().Get("status")),
		WorkflowType: r.URL.Query().Get("type"),
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *Server) deadLetters(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	jobs, err := s.store.List(r.Context(), engine.ListOptions{
		Limit:        limit,
		Queue:        r.URL.Query().Get("queue"),
		Status:       engine.StatusDeadLetter,
		WorkflowType: r.URL.Query().Get("type"),
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *Server) inspect(w http.ResponseWriter, r *http.Request) {
	job, events, err := s.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job, "events": events})
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Cancel(r.Context(), r.PathValue("id")); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "canceled"})
}

func (s *Server) replay(w http.ResponseWriter, r *http.Request) {
	job, err := s.store.Replay(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) retryDeadLetter(w http.ResponseWriter, r *http.Request) {
	job, err := s.store.RetryDeadLetter(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, engine.ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, engine.ErrInvalidTransition):
		writeError(w, http.StatusConflict, err)
	case errors.Is(err, engine.ErrInvalidStatus):
		writeError(w, http.StatusBadRequest, err)
	default:
		writeError(w, http.StatusInternalServerError, err)
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": strings.TrimSpace(err.Error())})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
