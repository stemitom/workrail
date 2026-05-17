package engine

import (
	"testing"
	"time"
)

func TestNextStatusAfterFailure(t *testing.T) {
	tests := []struct {
		name        string
		attempt     int
		maxAttempts int
		want        Status
	}{
		{name: "retry when attempts remain", attempt: 1, maxAttempts: 3, want: StatusRetrying},
		{name: "dead letter on final attempt", attempt: 3, maxAttempts: 3, want: StatusDeadLetter},
		{name: "dead letter when attempts exceeded", attempt: 4, maxAttempts: 3, want: StatusDeadLetter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NextStatusAfterFailure(tt.attempt, tt.maxAttempts); got != tt.want {
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

func TestIsValidStatus(t *testing.T) {
	if !IsValidStatus(StatusQueued) {
		t.Fatal("queued should be valid")
	}
	if IsValidStatus(Status("nope")) {
		t.Fatal("unknown status should be invalid")
	}
}
