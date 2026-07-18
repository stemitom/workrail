// Command embedded shows how to use the Workrail SDK inside an application:
// register a workflow, enqueue a job, and run a worker until interrupted.
package main

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stemitom/workrail"
)

type greetPayload struct {
	Name string `json:"name"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := workrail.Open(ctx, workrail.Options{
		DatabaseURL: os.Getenv("DATABASE_URL"),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Each Step checkpoints its result: if the workflow fails and retries, or
	// the worker dies mid-run, completed steps are not executed again.
	client.Register("greet", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		var p greetPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, err
		}
		greeting, err := workrail.Step(ctx, "compose", func(context.Context) (string, error) {
			return "hello " + p.Name, nil
		})
		if err != nil {
			return nil, err
		}
		if _, err := workrail.Step(ctx, "deliver", func(context.Context) (bool, error) {
			slog.Info("greeting", "message", greeting)
			return true, nil
		}); err != nil {
			return nil, err
		}
		return json.Marshal(map[string]string{"greeting": greeting})
	})

	job, inserted, err := client.EnqueueJSON(ctx, "greet", greetPayload{Name: "workrail"},
		workrail.WithIdempotencyKey("greet-example"),
		workrail.WithMaxAttempts(3),
	)
	if err != nil {
		log.Fatal(err)
	}
	slog.Info("enqueued", "job_id", job.ID, "inserted", inserted)

	if err := client.RunWorker(ctx, workrail.WorkerOptions{
		Queue:           "default",
		Concurrency:     2,
		LeaseDuration:   30 * time.Second,
		RetentionPeriod: 7 * 24 * time.Hour,
	}); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
