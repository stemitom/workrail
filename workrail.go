// Package workrail exposes the public Go SDK for embedding Workrail clients and
// workers in application services.
package workrail

import (
	"context"
	"encoding/json"
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
