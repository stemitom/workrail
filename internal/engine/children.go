package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ExecuteChild enqueues a child workflow and suspends the parent until it
// reaches a terminal state, without holding a worker slot. The child job ID
// is checkpointed under "child:"+name, so every resumed run and retry watches
// the same child; the idempotency key makes even the enqueue-crash window
// safe. The child runs on the parent's queue with a default attempt budget.
//
// A succeeded child returns its result. A dead-lettered or canceled child
// fails the parent permanently: retrying the parent would only re-watch a
// finished child, so retry the child itself first. Keep names unique per
// child, like Steps. Do not call ExecuteChild inside a Step function: the
// suspension must abort the workflow run, not fail the step.
func ExecuteChild(ctx context.Context, name, workflowType string, input json.RawMessage) (json.RawMessage, error) {
	runner, ok := ctx.Value(stepRunnerKey{}).(stepRunner)
	if !ok {
		return nil, errors.New("workrail: ExecuteChild requires a worker")
	}
	if len(input) == 0 {
		input = json.RawMessage(`{}`)
	}
	rawID, err := RunStep(ctx, "__child:"+name, func(context.Context) (json.RawMessage, error) {
		job, _, err := runner.store.Enqueue(ctx, EnqueueRequest{
			Queue:          runner.queue,
			WorkflowType:   workflowType,
			Payload:        input,
			IdempotencyKey: "child:" + runner.jobID + ":" + name,
			ParentID:       runner.jobID,
		})
		if err != nil {
			return nil, err
		}
		return json.Marshal(job.ID)
	})
	if err != nil {
		return nil, err
	}
	var childID string
	if err := json.Unmarshal(rawID, &childID); err != nil {
		return nil, fmt.Errorf("child %q: decode checkpoint: %w", name, err)
	}
	child, err := runner.store.GetJob(ctx, childID)
	if err != nil {
		return nil, fmt.Errorf("child %q: %w", name, err)
	}
	switch child.Status {
	case StatusSucceeded:
		if len(child.Result) == 0 {
			return json.RawMessage(`{}`), nil
		}
		return child.Result, nil
	case StatusDeadLetter, StatusCanceled:
		return nil, Permanent(fmt.Errorf("child workflow %q ended as %s", name, child.Status))
	default:
		return nil, &SuspendError{RunAfter: time.Now().UTC().Add(signalPollInterval)}
	}
}
