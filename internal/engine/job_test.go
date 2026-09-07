package engine

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestNextStatusAfterFailure(t *testing.T) {
	tests := []struct {
		name        string
		attempt     int
		maxAttempts int
		cause       error
		want        Status
	}{
		{name: "retry when attempts remain", attempt: 1, maxAttempts: 3, cause: errors.New("boom"), want: StatusRetrying},
		{name: "dead letter on final attempt", attempt: 3, maxAttempts: 3, cause: errors.New("boom"), want: StatusDeadLetter},
		{name: "dead letter when attempts exceeded", attempt: 4, maxAttempts: 3, cause: errors.New("boom"), want: StatusDeadLetter},
		{name: "dead letter permanent failure with attempts left", attempt: 1, maxAttempts: 5, cause: Permanent(errors.New("account closed")), want: StatusDeadLetter},
		{name: "dead letter wrapped permanent failure", attempt: 1, maxAttempts: 5, cause: fmt.Errorf("step %q: %w", "charge", Permanent(errors.New("account closed"))), want: StatusDeadLetter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NextStatusAfterFailure(tt.attempt, tt.maxAttempts, tt.cause); got != tt.want {
				t.Fatalf("NextStatusAfterFailure() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestBackoff(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: time.Second},
		{attempt: 1, want: time.Second},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 7, want: time.Minute},
	}
	for _, tt := range tests {
		if got := Backoff(tt.attempt); got != tt.want {
			t.Fatalf("Backoff(%d) = %s, want %s", tt.attempt, got, tt.want)
		}
	}
}

func TestNormalizeEnqueueDefaultsQueue(t *testing.T) {
	req := NormalizeEnqueue(EnqueueRequest{WorkflowType: "echo"})
	if req.Queue != "default" {
		t.Fatalf("queue = %q, want default", req.Queue)
	}
}

func TestIsValidStatus(t *testing.T) {
	if !IsValidStatus(StatusQueued) {
		t.Fatal("queued should be valid")
	}
	if IsValidStatus(Status("nope")) {
		t.Fatal("unknown status should be invalid")
	}
}

func TestPermanentPreservesCause(t *testing.T) {
	cause := errors.New("insufficient funds")
	err := Permanent(cause)
	if !IsPermanent(err) {
		t.Fatal("Permanent() error should report as permanent")
	}
	if !errors.Is(err, cause) {
		t.Fatal("Permanent() error should unwrap to its cause")
	}
	if err.Error() != cause.Error() {
		t.Fatalf("Error() = %q, want %q", err.Error(), cause.Error())
	}
	if IsPermanent(cause) {
		t.Fatal("plain error should not report as permanent")
	}
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) should be nil")
	}
}
