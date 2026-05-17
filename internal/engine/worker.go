package engine

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
)

type Worker struct {
	ID            string
	Store         Store
	Registry      *Registry
	PollInterval  time.Duration
	LeaseDuration time.Duration
	Concurrency   int
	Logger        *slog.Logger
}

func (w *Worker) Run(ctx context.Context) error {
	if w.PollInterval == 0 {
		w.PollInterval = time.Second
	}
	if w.LeaseDuration == 0 {
		w.LeaseDuration = 30 * time.Second
	}
	if w.Concurrency <= 0 {
		w.Concurrency = 4
	}
	if w.Logger == nil {
		w.Logger = slog.Default()
	}

	sem := make(chan struct{}, w.Concurrency)
	ticker := time.NewTicker(w.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		jobs, err := w.Store.Claim(ctx, ClaimOptions{
			WorkerID:      w.ID,
			LeaseDuration: w.LeaseDuration,
			Limit:         w.Concurrency,
		})
		if err != nil {
			w.Logger.Error("claim failed", "error", err)
		}

		for _, job := range jobs {
			sem <- struct{}{}
			go func(job Job) {
				defer func() { <-sem }()
				w.runJob(ctx, job)
			}(job)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *Worker) runJob(parent context.Context, job Job) {
	tracer := otel.Tracer("workrail/worker")
	ctx, span := tracer.Start(parent, "workflow.execute")
	defer span.End()

	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	heartbeatDone := make(chan struct{})
	go func() {
		t := time.NewTicker(w.LeaseDuration / 3)
		defer t.Stop()
		defer close(heartbeatDone)
		for {
			select {
			case <-jobCtx.Done():
				return
			case <-t.C:
				if err := w.Store.Heartbeat(parent, job.ID, w.ID); err != nil {
					w.Logger.Warn("heartbeat failed", "job_id", job.ID, "error", err)
				}
			}
		}
	}()

	w.Logger.Info("job started", "job_id", job.ID, "workflow_type", job.WorkflowType, "attempt", job.Attempt)
	result, err := w.Registry.Execute(jobCtx, job.WorkflowType, job.Payload)
	cancel()
	<-heartbeatDone

	if err != nil {
		w.Logger.Warn("job failed", "job_id", job.ID, "error", err)
		if failErr := w.Store.Fail(parent, job.ID, w.ID, err); failErr != nil {
			w.Logger.Error("record failure failed", "job_id", job.ID, "error", failErr)
		}
		return
	}

	if err := w.Store.Complete(parent, job.ID, w.ID, result); err != nil {
		w.Logger.Error("complete failed", "job_id", job.ID, "error", err)
		return
	}
	w.Logger.Info("job succeeded", "job_id", job.ID)
}
