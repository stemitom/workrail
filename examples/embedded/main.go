// Command embedded shows how to use the Workrail SDK inside an application:
// register a workflow, enqueue a job, and run a worker until interrupted.
package main

import (
	"context"
	"encoding/json"
	"fmt"
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
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer client.Close()

	client.Register("greet", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		var p greetPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, err
		}
		slog.Info("greeting", "name", p.Name)
		return json.Marshal(map[string]string{"greeting": "hello " + p.Name})
	})

	job, inserted, err := client.EnqueueJSON(ctx, "greet", greetPayload{Name: "workrail"},
		workrail.WithIdempotencyKey("greet-example"),
		workrail.WithMaxAttempts(3),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	slog.Info("enqueued", "job_id", job.ID, "inserted", inserted)

	if err := client.RunWorker(ctx, workrail.WorkerOptions{
		Queue:           "default",
		Concurrency:     2,
		LeaseDuration:   30 * time.Second,
		RetentionPeriod: 7 * 24 * time.Hour,
	}); err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
