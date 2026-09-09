package engine

import (
	"context"
	"encoding/json"
)

// RegisterCompensation adds the unwind hook for a workflow type. It runs once
// when a job of that type dead-letters — after Fail puts it there, or when
// the lease-expiry sweep dead-letters a crash-looping job. The compensator
// gets the dead job's payload and runs outside any lease, so Step/Sleep/
// WaitSignal fail inside it (their checkpoints need a live lease): keep it to
// plain idempotent activities, including pure ExecuteActivity calls. Its
// outcome is recorded as a job.compensated or job.compensation_failed event.
// Types without a compensator dead-letter silently, as before.
func (r *Registry) RegisterCompensation(workflowType string, fn WorkflowFunc) {
	if r.compensations == nil {
		r.compensations = map[string]WorkflowFunc{}
	}
	r.compensations[workflowType] = fn
}

// compensate runs the dead-letter hook for jobID, if the job dead-lettered
// and its type has one. Safe to call on any job: anything else is a no-op.
func (w *Worker) compensate(ctx context.Context, jobID string) {
	job, _, err := w.Store.Get(ctx, jobID)
	if err != nil {
		w.logger().Error("compensation lookup failed", "job_id", jobID, "error", err)
		return
	}
	if job.Status != StatusDeadLetter {
		return
	}
	fn, ok := w.Registry.compensations[job.WorkflowType]
	if !ok {
		return
	}
	// A fresh context: the job's lease is gone, and the run's context is
	// already canceled by the time compensation triggers. No checkpoint
	// frame: Step/Sleep/WaitSignal need a live lease and fail without one,
	// though pure ExecuteActivity calls (no inner Steps) work.
	cctx := WithRegistry(WithStepRunner(context.Background(), w.Store, job.ID, w.ID, job.Queue), w.Registry)
	record := func(event string, err error) {
		msg := ""
		if err != nil {
			msg = err.Error()
		}
		details, _ := json.Marshal(map[string]any{"error": msg})
		if rerr := w.Store.RecordEvent(context.Background(), jobID, event, details); rerr != nil {
			w.logger().Error("compensation event failed", "job_id", jobID, "error", rerr)
		}
	}
	w.logger().Info("compensating dead-lettered job", "job_id", jobID, "workflow_type", job.WorkflowType)
	if _, err := fn(cctx, job.Payload); err != nil {
		w.logger().Error("compensation failed", "job_id", jobID, "error", err)
		record("job.compensation_failed", err)
		return
	}
	record("job.compensated", nil)
}
