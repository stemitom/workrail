package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
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
	authToken string
}

func New(store engine.Store, logger *slog.Logger, authToken string) *Server {
	s := &Server{store: store, logger: logger, mux: http.NewServeMux(), authToken: authToken}
	observability.RegisterQueueDepthCollector(store)
	s.routes()
	if authToken == "" {
		logger.Warn("api auth token not configured; all endpoints are unauthenticated")
	}
	return s
}

func (s *Server) Handler() http.Handler {
	return otelhttp.NewHandler(observability.HTTPMetrics(s.auth(s.mux)), "workrail.http")
}

// auth exempts /healthz so load balancers can probe without credentials.
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.authToken == "" || r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(s.authToken)) != 1 {
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
