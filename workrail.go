// Package workrail exposes the public Go SDK for embedding Workrail clients and
// workers in application services.
package workrail

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/stemitom/workrail/internal/engine"
	"github.com/stemitom/workrail/internal/store/postgres"
)

const DefaultDatabaseURL = "postgres://durable:durable@localhost:5432/durable?sslmode=disable"

type Status = engine.Status

const (
	StatusQueued     = engine.StatusQueued
	StatusRunning    = engine.StatusRunning
	StatusRetrying   = engine.StatusRetrying
	StatusSucceeded  = engine.StatusSucceeded
	StatusFailed     = engine.StatusFailed
	StatusDeadLetter = engine.StatusDeadLetter
	StatusCanceled   = engine.StatusCanceled
)

// ErrPermanent marks a failure retrying cannot fix. Match it with
// IsPermanent; produce it with Permanent.
var ErrPermanent = engine.ErrPermanent

// Permanent wraps err so the worker dead-letters the job immediately instead
// of spending its remaining attempts. Use it for rejections that will repeat
// identically — a closed account, a failed validation, a provider's 4xx — and
// leave transient failures unwrapped so backoff and retries still apply. The
// returned error keeps err's message and unwraps to it.
func Permanent(err error) error { return engine.Permanent(err) }

// IsPermanent reports whether err, or anything it wraps, was marked permanent.
func IsPermanent(err error) bool { return engine.IsPermanent(err) }

type Job = engine.Job
type Event = engine.Event
type EnqueueRequest = engine.EnqueueRequest
type ListOptions = engine.ListOptions
type QueueDepth = engine.QueueDepth
type WorkflowFunc = engine.WorkflowFunc

type Options struct {
	DatabaseURL string
	Logger      *slog.Logger
}

type WorkerOptions struct {
	ID              string
	Queue           string
	PollInterval    time.Duration
	LeaseDuration   time.Duration
	ShutdownTimeout time.Duration
	// RetentionPeriod prunes succeeded and canceled jobs during sweeps; zero disables pruning.
	RetentionPeriod time.Duration
	Concurrency     int
}

type Client struct {
	store    engine.Store
	registry *engine.Registry
	logger   *slog.Logger
	closer   interface{ Close() }
}

func Open(ctx context.Context, opts Options) (*Client, error) {
	databaseURL := opts.DatabaseURL
	if databaseURL == "" {
		databaseURL = DefaultDatabaseURL
	}
	store, err := postgres.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Client{
		store:    store,
		registry: engine.NewRegistry(),
		logger:   logger,
		closer:   store,
	}, nil
}

func (c *Client) Close() {
	if c.closer != nil {
		c.closer.Close()
	}
}

func (c *Client) Register(name string, workflow WorkflowFunc) {
	c.registry.Register(name, workflow)
}

func (c *Client) Enqueue(ctx context.Context, req EnqueueRequest) (Job, bool, error) {
	return c.store.Enqueue(ctx, req)
}

func (c *Client) EnqueueJSON(ctx context.Context, workflowType string, payload any, opts ...EnqueueOption) (Job, bool, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return Job{}, false, err
	}
	req := EnqueueRequest{Queue: "default", WorkflowType: workflowType, Payload: data}
	for _, opt := range opts {
		opt(&req)
	}
	return c.Enqueue(ctx, req)
}

func (c *Client) Get(ctx context.Context, jobID string) (Job, []Event, error) {
	return c.store.Get(ctx, jobID)
}

func (c *Client) List(ctx context.Context, limit int) ([]Job, error) {
	return c.store.List(ctx, ListOptions{Limit: limit})
}

func (c *Client) ListJobs(ctx context.Context, opts ListOptions) ([]Job, error) {
	return c.store.List(ctx, opts)
}

func (c *Client) QueueDepth(ctx context.Context) ([]QueueDepth, error) {
	return c.store.QueueDepth(ctx)
}

func (c *Client) Cancel(ctx context.Context, jobID string) error {
	return c.store.Cancel(ctx, jobID)
}

// Signal delivers payload to a workflow waiting on name. The payload is
// checkpointed on first delivery, so late or duplicate signals under the same
// name are ignored. Signaling a finished job fails.
func (c *Client) Signal(ctx context.Context, jobID, name string, payload any) error {
	var data []byte
	switch p := payload.(type) {
	case json.RawMessage:
		data = p
	case []byte:
		data = p
	default:
		var err error
		data, err = json.Marshal(payload)
		if err != nil {
			return err
		}
	}
	return c.store.Signal(ctx, jobID, name, data)
}

func (c *Client) RetryDeadLetter(ctx context.Context, jobID string) (Job, error) {
	return c.store.RetryDeadLetter(ctx, jobID)
}

func (c *Client) Replay(ctx context.Context, jobID string) (Job, error) {
	return c.store.Replay(ctx, jobID)
}

func (c *Client) RunWorker(ctx context.Context, opts WorkerOptions) error {
	workerID := opts.ID
	if workerID == "" {
		hostname, _ := os.Hostname()
		workerID = hostname
	}
	if workerID == "" {
		workerID = "worker"
	}
	return (&engine.Worker{
		ID:              workerID,
		Queue:           opts.Queue,
		Store:           c.store,
		Registry:        c.registry,
		PollInterval:    opts.PollInterval,
		LeaseDuration:   opts.LeaseDuration,
		ShutdownTimeout: opts.ShutdownTimeout,
		RetentionPeriod: opts.RetentionPeriod,
		Concurrency:     opts.Concurrency,
		Logger:          c.logger,
	}).Run(ctx)
}

// Step checkpoints fn's result per job, so a retried workflow resumes after
// its last completed step instead of redoing work. The guarantee is
// effectively once, not exactly once: a crash between fn's side effect and
// the checkpoint commit re-runs the step on retry, so side effects that must
// not repeat should be idempotent. Steps are identified by name within a job;
// keep names, and T's JSON shape, stable while jobs are in flight. T must
// round-trip through JSON — results are stored as normalized jsonb, so byte
// layout and key order are not preserved.
func Step[T any](ctx context.Context, name string, fn func(context.Context) (T, error)) (T, error) {
	var value T
	raw, err := engine.RunStep(ctx, name, func(ctx context.Context) (json.RawMessage, error) {
		result, err := fn(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(result)
	})
	if err != nil {
		return value, err
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return value, fmt.Errorf("step %q: decode checkpoint: %w", name, err)
	}
	return value, nil
}

// Sleep waits d without holding a worker slot. See engine.Sleep for the
// suspend/resume contract: names must be stable and unique per workflow, and
// Sleep must run directly in the workflow, not inside a Step function.
func Sleep(ctx context.Context, name string, d time.Duration) error {
	return engine.Sleep(ctx, name, d)
}

// WaitSignal waits for the first signal delivered under name, without holding
// a worker slot. The delivered payload is checkpointed, so every resumed run
// and retry sees the same value; keep names unique per wait, like Steps. It
// must run directly in the workflow, not inside a Step function.
func WaitSignal[T any](ctx context.Context, name string) (T, error) {
	return WaitSignalAt[T](ctx, name, 0)
}

// WaitSignalAt waits for the index-th signal delivered under name (0-based,
// in delivery order), so a workflow can consume a stream of signals in order.
// Each delivery checkpoints under its own index; a wait past the last
// delivery suspends until more signals arrive.
func WaitSignalAt[T any](ctx context.Context, name string, index int) (T, error) {
	var value T
	raw, err := engine.WaitSignalAt(ctx, name, index)
	if err != nil {
		return value, err
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return value, fmt.Errorf("signal %q: decode payload: %w", name, err)
	}
	return value, nil
}

type EnqueueOption func(*EnqueueRequest)

func WithIdempotencyKey(key string) EnqueueOption {
	return func(req *EnqueueRequest) {
		req.IdempotencyKey = key
	}
}

func WithMaxAttempts(maxAttempts int) EnqueueOption {
	return func(req *EnqueueRequest) {
		req.MaxAttempts = maxAttempts
	}
}

func WithRunAfter(runAfter time.Time) EnqueueOption {
	return func(req *EnqueueRequest) {
		req.RunAfter = runAfter
	}
}

func WithQueue(queue string) EnqueueOption {
	return func(req *EnqueueRequest) {
		req.Queue = queue
	}
}
