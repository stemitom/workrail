package engine

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/stemitom/workrail/internal/observability"
)

type Status string

const (
	StatusQueued     Status = "queued"
	StatusRunning    Status = "running"
	StatusRetrying   Status = "retrying"
	StatusSucceeded  Status = "succeeded"
	StatusFailed     Status = "failed"
	StatusDeadLetter Status = "dead_letter"
	StatusCanceled   Status = "canceled"
)

type Job struct {
	ID             string          `json:"id"`
	Queue          string          `json:"queue"`
	WorkflowType   string          `json:"workflow_type"`
	Payload        json.RawMessage `json:"payload"`
	Status         Status          `json:"status"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAfter       time.Time       `json:"run_after"`
	LeaseOwner     *string         `json:"lease_owner,omitempty"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	HeartbeatAt    *time.Time      `json:"heartbeat_at,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
	Error          *string         `json:"error,omitempty"`
	TraceID        *string         `json:"trace_id,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
}

type Event struct {
	ID        int64           `json:"id"`
	JobID     string          `json:"job_id"`
	EventType string          `json:"event_type"`
	Details   json.RawMessage `json:"details"`
	CreatedAt time.Time       `json:"created_at"`
}

type QueueDepth = observability.QueueDepth

type EnqueueRequest struct {
	Queue          string          `json:"queue"`
	WorkflowType   string          `json:"workflow_type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAfter       time.Time       `json:"run_after"`
}

type ClaimOptions struct {
	WorkerID      string
	Queue         string
	LeaseDuration time.Duration
	Limit         int
}

type ListOptions struct {
	Limit        int
	Queue        string
	Status       Status
	WorkflowType string
	// BeforeCreatedAt/BeforeID form a keyset cursor: only jobs strictly older
	// than (created_at, id) are returned. Both must be set together.
	BeforeCreatedAt time.Time
	BeforeID        string
}

var (
	ErrNotFound          = errors.New("job not found")
	ErrInvalidTransition = errors.New("invalid job state transition")
	ErrInvalidStatus     = errors.New("invalid job status")
)

func NormalizeEnqueue(req EnqueueRequest) EnqueueRequest {
	if req.Queue == "" {
		req.Queue = "default"
	}
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{}`)
	}
	if req.MaxAttempts <= 0 {
		req.MaxAttempts = 3
	}
	if req.RunAfter.IsZero() {
		req.RunAfter = time.Now().UTC()
	}
	return req
}

func NextStatusAfterFailure(attempt, maxAttempts int) Status {
	if attempt >= maxAttempts {
		return StatusDeadLetter
	}
	return StatusRetrying
}

func Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := time.Second * time.Duration(1<<(attempt-1))
	if delay > time.Minute {
		return time.Minute
	}
	return delay
}

func IsValidStatus(status Status) bool {
	switch status {
	case "", StatusQueued, StatusRunning, StatusRetrying, StatusSucceeded, StatusFailed, StatusDeadLetter, StatusCanceled:
		return true
	default:
		return false
	}
}
