package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/stemitom/workrail/internal/observability"

	"go.opentelemetry.io/otel"
)

// drainGrace bounds how long a timed-out drain waits for canceled jobs to record their failures.
const drainGrace = 5 * time.Second

type Worker struct {
	ID              string
	Queue           string
	Store           Store
	Registry        *Registry
	PollInterval    time.Duration
	LeaseDuration   time.Duration
	ShutdownTimeout time.Duration
	// RetentionPeriod prunes succeeded and canceled jobs during sweeps; zero disables pruning.
	RetentionPeriod time.Duration
	Concurrency     int
	Logger          *slog.Logger
}

func (w *Worker) Run(ctx context.Context) error {
	w.applyDefaults()
	observability.WorkerConfiguredConcurrency.WithLabelValues(w.ID, w.Queue).Set(float64(w.Concurrency))

	execCtx, cancelExec := context.WithCancel(context.Background())
	defer cancelExec()
	sem := make(chan struct{}, w.Concurrency)
	var wg sync.WaitGroup
	ticker := time.NewTicker(w.PollInterval)
	defer ticker.Stop()
	sweepTicker := time.NewTicker(w.LeaseDuration)
	defer sweepTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return w.drain(ctx, cancelExec, &wg)
		default:
		}

		var jobs []Job
		if free := w.Concurrency - len(sem); free > 0 {
			var err error
			jobs, err = w.Store.Claim(ctx, ClaimOptions{
				WorkerID:      w.ID,
				Queue:         w.Queue,
				LeaseDuration: w.LeaseDuration,
				Limit:         free,
			})
			if err != nil {
				w.logger().Error("claim failed", "error", err)
			}
		}

		for _, job := range jobs {
			observability.JobsClaimed.WithLabelValues(job.Queue, job.WorkflowType).Inc()
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
		case <-sweepTicker.C:
			if count, err := w.Store.DeadLetterExhausted(ctx); err != nil {
				w.logger().Error("dead-letter sweep failed", "error", err)
			} else if count > 0 {
				w.logger().Warn("dead-lettered jobs with expired leases and exhausted attempts", "count", count)
			}
			if w.RetentionPeriod > 0 {
				if count, err := w.Store.PruneCompleted(ctx, w.Queue, w.RetentionPeriod); err != nil {
					w.logger().Error("retention prune failed", "error", err)
				} else if count > 0 {
					w.logger().Info("pruned completed jobs", "count", count, "retention", w.RetentionPeriod)
				}
			}
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
		w.logger().Warn("worker drain timed out, canceling in-flight jobs", "timeout", w.ShutdownTimeout)
		cancelExec()
		select {
		case <-done:
		case <-time.After(drainGrace):
			w.logger().Warn("in-flight jobs did not stop after cancel", "grace", drainGrace)
		}
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
		interval := heartbeatInterval(w.LeaseDuration)
		t := time.NewTicker(interval)
		defer t.Stop()
		defer close(heartbeatDone)
		lastRenewed := time.Now()
		for {
			select {
			case <-jobCtx.Done():
				return
			case <-t.C:
				recordCtx, recordCancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := w.Store.Heartbeat(recordCtx, job.ID, w.ID, w.LeaseDuration)
				recordCancel()
				switch {
				case err == nil:
					lastRenewed = time.Now()
					observability.JobHeartbeats.WithLabelValues(job.Queue).Inc()
				case errors.Is(err, ErrInvalidTransition):
					w.logger().Warn("lease lost, canceling job", "job_id", job.ID)
					cancel()
					return
				default:
					w.logger().Warn("heartbeat failed", "job_id", job.ID, "error", err)
					// Stop before the unrenewed lease expires; another worker may own the job after that.
					if time.Since(lastRenewed) >= w.LeaseDuration-interval {
						w.logger().Warn("lease presumed lost after failed heartbeats, canceling job", "job_id", job.ID)
						cancel()
						return
					}
				}
			}
		}
	}()

	w.logger().Info("job started", "job_id", job.ID, "workflow_type", job.WorkflowType, "attempt", job.Attempt)
	observability.WorkerInFlightJobs.WithLabelValues(job.Queue, job.WorkflowType).Inc()
	defer observability.WorkerInFlightJobs.WithLabelValues(job.Queue, job.WorkflowType).Dec()
	result, err := w.executeSafely(jobCtx, job)
	cancel()
	<-heartbeatDone

	if err != nil {
		w.logger().Warn("job failed", "job_id", job.ID, "error", err)
		recordCtx, recordCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer recordCancel()
		if failErr := w.Store.Fail(recordCtx, job.ID, w.ID, err); failErr != nil {
			w.logger().Error("record failure failed", "job_id", job.ID, "error", failErr)
		} else {
			observability.JobsFailed.WithLabelValues(job.Queue, job.WorkflowType).Inc()
		}
		return
	}

	recordCtx, recordCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer recordCancel()
	if err := w.Store.Complete(recordCtx, job.ID, w.ID, result); err != nil {
		w.logger().Error("complete failed", "job_id", job.ID, "error", err)
		return
	}
	observability.JobsSucceeded.WithLabelValues(job.Queue, job.WorkflowType).Inc()
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
	if w.Queue == "" {
		w.Queue = "default"
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
