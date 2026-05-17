package engine

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
)

type Worker struct {
	ID              string
	Store           Store
	Registry        *Registry
	PollInterval    time.Duration
	LeaseDuration   time.Duration
	ShutdownTimeout time.Duration
	Concurrency     int
	Logger          *slog.Logger
}

func (w *Worker) Run(ctx context.Context) error {
	w.applyDefaults()

	execCtx, cancelExec := context.WithCancel(context.Background())
	defer cancelExec()
	sem := make(chan struct{}, w.Concurrency)
	var wg sync.WaitGroup
	ticker := time.NewTicker(w.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return w.drain(ctx, cancelExec, &wg)
		default:
		}

		jobs, err := w.Store.Claim(ctx, ClaimOptions{
			WorkerID:      w.ID,
			LeaseDuration: w.LeaseDuration,
			Limit:         w.Concurrency,
		})
		if err != nil {
			w.logger().Error("claim failed", "error", err)
		}

		for _, job := range jobs {
			sem <- struct{}{}
			wg.Add(1)
			go func(job Job) {
				defer func() {
					<-sem
					wg.Done()
				}()
				w.runJob(execCtx, job)
			}(job)
		}

		select {
		case <-ctx.Done():
			return w.drain(ctx, cancelExec, &wg)
		case <-ticker.C:
		}
	}
}

func (w *Worker) drain(ctx context.Context, cancelExec context.CancelFunc, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	select {
	case <-done:
		w.logger().Info("worker drained")
		return ctx.Err()
	case <-time.After(w.ShutdownTimeout):
		w.logger().Warn("worker drain timed out", "timeout", w.ShutdownTimeout)
		cancelExec()
		return ctx.Err()
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
		t := time.NewTicker(heartbeatInterval(w.LeaseDuration))
		defer t.Stop()
		defer close(heartbeatDone)
		for {
			select {
			case <-jobCtx.Done():
				return
			case <-t.C:
				recordCtx, recordCancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := w.Store.Heartbeat(recordCtx, job.ID, w.ID); err != nil {
					w.logger().Warn("heartbeat failed", "job_id", job.ID, "error", err)
				}
				recordCancel()
			}
		}
	}()

	w.logger().Info("job started", "job_id", job.ID, "workflow_type", job.WorkflowType, "attempt", job.Attempt)
	result, err := w.executeSafely(jobCtx, job)
	cancel()
	<-heartbeatDone

	if err != nil {
		w.logger().Warn("job failed", "job_id", job.ID, "error", err)
		recordCtx, recordCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer recordCancel()
		if failErr := w.Store.Fail(recordCtx, job.ID, w.ID, err); failErr != nil {
			w.logger().Error("record failure failed", "job_id", job.ID, "error", failErr)
		}
		return
	}

	recordCtx, recordCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer recordCancel()
	if err := w.Store.Complete(recordCtx, job.ID, w.ID, result); err != nil {
		w.logger().Error("complete failed", "job_id", job.ID, "error", err)
		return
	}
	w.logger().Info("job succeeded", "job_id", job.ID)
}

func (w *Worker) applyDefaults() {
	if w.PollInterval == 0 {
		w.PollInterval = time.Second
	}
	if w.LeaseDuration == 0 {
		w.LeaseDuration = 30 * time.Second
	}
	if w.ShutdownTimeout == 0 {
		w.ShutdownTimeout = 30 * time.Second
	}
	if w.Concurrency <= 0 {
		w.Concurrency = 4
	}
	if w.Logger == nil {
		w.Logger = slog.Default()
	}
}

func (w *Worker) logger() *slog.Logger {
	if w.Logger == nil {
		return slog.Default()
	}
	return w.Logger
}

func (w *Worker) executeSafely(ctx context.Context, job Job) (result []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("workflow panic: %v\n%s", recovered, debug.Stack())
		}
	}()
	return w.Registry.Execute(ctx, job.WorkflowType, job.Payload)
}

func heartbeatInterval(leaseDuration time.Duration) time.Duration {
	if leaseDuration <= 0 {
		return 10 * time.Second
	}
	interval := leaseDuration / 3
	if interval <= 0 {
		return time.Millisecond
	}
	return interval
}
