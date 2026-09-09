package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

type WorkflowFunc func(context.Context, json.RawMessage) (json.RawMessage, error)

// ActivityFunc documents the activity contract: side effects live here,
// orchestration (Step, Sleep, WaitSignal, ExecuteActivity) in WorkflowFunc.
// Same shape, kept as an alias — the honesty is in which Register takes it.
type ActivityFunc = WorkflowFunc

type Registry struct {
	workflows     map[string]WorkflowFunc
	activities    map[string]ActivityFunc
	compensations map[string]WorkflowFunc
}

func NewRegistry() *Registry {
	r := &Registry{workflows: map[string]WorkflowFunc{}, activities: map[string]ActivityFunc{}}
	r.RegisterActivity("echo", echoWorkflow)
	r.RegisterActivity("sleep", sleepWorkflow)
	r.Register("sequence", sequenceWorkflow())
	return r
}

// Register adds a workflow (orchestration). RegisterActivity adds a
// side-effecting activity.
func (r *Registry) Register(name string, wf WorkflowFunc) {
	r.workflows[name] = wf
}

func (r *Registry) RegisterActivity(name string, fn ActivityFunc) {
	r.activities[name] = fn
}

func (r *Registry) Execute(ctx context.Context, typ string, payload json.RawMessage) (json.RawMessage, error) {
	if wf, ok := r.workflows[typ]; ok {
		return wf(ctx, payload)
	}
	// Single-activity jobs: enqueueing an activity type runs it directly.
	if fn, ok := r.activities[typ]; ok {
		return fn(ctx, payload)
	}
	return nil, fmt.Errorf("unknown workflow type %q", typ)
}

func echoWorkflow(_ context.Context, payload json.RawMessage) (json.RawMessage, error) {
	if len(payload) == 0 {
		return json.RawMessage(`{}`), nil
	}
	return payload, nil
}

func sleepWorkflow(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var req struct {
		Seconds int `json:"seconds" yaml:"seconds"`
	}
	if err := decodeJSONOrYAML(payload, &req); err != nil {
		return nil, err
	}
	if req.Seconds < 1 {
		req.Seconds = 1
	}
	select {
	case <-time.After(time.Duration(req.Seconds) * time.Second):
		return json.Marshal(map[string]any{"slept_seconds": req.Seconds})
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func sequenceWorkflow() WorkflowFunc {
	return func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		var spec struct {
			Steps []struct {
				Name     string          `json:"name" yaml:"name"`
				Activity string          `json:"activity" yaml:"activity"`
				Input    json.RawMessage `json:"input" yaml:"input"`
			} `json:"steps" yaml:"steps"`
		}
		if err := decodeJSONOrYAML(payload, &spec); err != nil {
			return nil, err
		}
		results := map[string]json.RawMessage{}
		for i, step := range spec.Steps {
			if step.Name == "" {
				step.Name = fmt.Sprintf("step_%d", i+1)
			}
			// A duplicate name would silently return the first step's
			// checkpoint instead of running this step's activity.
			if _, exists := results[step.Name]; exists {
				return nil, fmt.Errorf("duplicate step name %q", step.Name)
			}
			result, err := RunStep(ctx, step.Name, func(ctx context.Context) (json.RawMessage, error) {
				return ExecuteActivity(pushFrame(ctx, step.Name), step.Activity, step.Input)
			})
			if err != nil {
				return nil, err
			}
			results[step.Name] = result
		}
		return json.Marshal(results)
	}
}

func decodeJSONOrYAML(data []byte, target any) error {
	if len(data) == 0 {
		return nil
	}
	if json.Valid(data) {
		return json.Unmarshal(data, target)
	}
	return yaml.Unmarshal(data, target)
}
