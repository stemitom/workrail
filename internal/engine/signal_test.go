package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestWaitSignalSuspendsWhenEmpty(t *testing.T) {
	store := &workerTestStore{}
	ctx := WithStepRunner(context.Background(), store, "job-1", "worker-a")

	_, err := WaitSignal(ctx, "approval")
	var suspend *SuspendError
	if !errors.As(err, &suspend) {
		t.Fatalf("WaitSignal = %v, want suspend", err)
	}
	if until := time.Until(suspend.RunAfter); until < 10*time.Second || until > time.Minute {
		t.Fatalf("nap = %v, want ~15s", until)
	}
}

func TestWaitSignalDeliversOnce(t *testing.T) {
	store := &workerTestStore{}
	ctx := WithStepRunner(context.Background(), store, "job-1", "worker-a")

	if err := store.Signal(context.Background(), "job-1", "approval", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatalf("signal: %v", err)
	}
	first, err := WaitSignal(ctx, "approval")
	if err != nil {
		t.Fatalf("first wait: %v", err)
	}
	if string(first) != `{"ok":true}` {
		t.Fatalf("payload = %s, want checkpointed signal", first)
	}

	// A late duplicate under the same name must not replace the delivery:
	// the mailbox keeps earliest-wins order and the checkpoint freezes it.
	if err := store.Signal(context.Background(), "job-1", "approval", json.RawMessage(`{"ok":false}`)); err != nil {
		t.Fatalf("duplicate signal: %v", err)
	}
	second, err := WaitSignal(ctx, "approval")
	if err != nil {
		t.Fatalf("second wait: %v", err)
	}
	if string(second) != string(first) {
		t.Fatalf("second = %s, want first delivery %s", second, first)
	}
}

func TestWaitSignalNeedsWorker(t *testing.T) {
	if _, err := WaitSignal(context.Background(), "approval"); err == nil {
		t.Fatal("WaitSignal without a runner should fail, not block")
	}
}
