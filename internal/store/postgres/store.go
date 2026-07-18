package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/stemitom/workrail/internal/engine"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	db *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Store, error) {
	db, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() {
	s.db.Close()
}

// withTx runs fn in a transaction, rolling back on error and committing otherwise.
func (s *Store) withTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// tryAdvisoryLock lets concurrent workers skip janitor work another worker is
// already doing; the lock releases with the transaction.
func tryAdvisoryLock(ctx context.Context, tx pgx.Tx, key string) (bool, error) {
	var held bool
	err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext($1))`, key).Scan(&held)
	return held, err
}

func (s *Store) Enqueue(ctx context.Context, req engine.EnqueueRequest) (engine.Job, bool, error) {
	req = engine.NormalizeEnqueue(req)
	var job engine.Job
	inserted := false
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `
		INSERT INTO jobs (queue, workflow_type, payload, idempotency_key, max_attempts, run_after)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6)
		ON CONFLICT (idempotency_key) DO UPDATE SET updated_at = jobs.updated_at
		RETURNING id, queue, workflow_type, payload, status, idempotency_key, attempt, max_attempts, run_after,
			lease_owner, lease_expires_at, heartbeat_at, result, error, trace_id, created_at, updated_at, completed_at,
			(xmax = 0) AS inserted
	`, req.Queue, req.WorkflowType, req.Payload, req.IdempotencyKey, req.MaxAttempts, req.RunAfter).
			Scan(append(jobScanTargets(&job), &inserted)...); err != nil {
			return err
		}
		event := "job.enqueued"
		if !inserted {
			event = "job.enqueue_idempotent_hit"
		}
		return appendEventTx(ctx, tx, job.ID, event, map[string]any{"queue": req.Queue, "workflow_type": req.WorkflowType})
	})
	if err != nil {
		return engine.Job{}, false, err
	}
	return job, inserted, nil
}

func (s *Store) Claim(ctx context.Context, opts engine.ClaimOptions) ([]engine.Job, error) {
	if opts.Limit <= 0 {
		opts.Limit = 1
	}
	if opts.Queue == "" {
		opts.Queue = "default"
	}
	var jobs []engine.Job
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
		WITH claimable AS (
			SELECT id
			FROM jobs
			WHERE (
				queue = $4 AND status IN ('queued', 'retrying') AND run_after <= now()
			) OR (
				queue = $4 AND status = 'running' AND lease_expires_at < now() AND attempt < max_attempts
			)
			ORDER BY run_after, created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE jobs j
		SET status = 'running',
			attempt = j.attempt + 1,
			lease_owner = $2,
			lease_expires_at = now() + $3::interval,
			heartbeat_at = now(),
			updated_at = now(),
			error = NULL
		FROM claimable
		WHERE j.id = claimable.id
		RETURNING j.id, j.queue, j.workflow_type, j.payload, j.status, j.idempotency_key, j.attempt, j.max_attempts, j.run_after,
			j.lease_owner, j.lease_expires_at, j.heartbeat_at, j.result, j.error, j.trace_id, j.created_at, j.updated_at, j.completed_at
	`, opts.Limit, opts.WorkerID, pgInterval(opts.LeaseDuration), opts.Queue)
		if err != nil {
			return err
		}
		jobs, err = scanJobs(rows)
		if err != nil {
			return err
		}
		for _, job := range jobs {
			if err := appendEventTx(ctx, tx, job.ID, "job.claimed", map[string]any{"worker_id": opts.WorkerID, "attempt": job.Attempt}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func (s *Store) DeadLetterExhausted(ctx context.Context) (int, error) {
	count := 0
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		held, err := tryAdvisoryLock(ctx, tx, "workrail:dead_letter_sweep")
		if err != nil || !held {
			return err
		}
		deadLettered, err := queryStrings(ctx, tx, `
			WITH exhausted AS (
				SELECT id
				FROM jobs
				WHERE status = 'running' AND lease_expires_at < now() AND attempt >= max_attempts
				FOR UPDATE SKIP LOCKED
			)
			UPDATE jobs j
			SET status = 'dead_letter',
				error = COALESCE(j.error, 'lease expired after max attempts'),
				lease_owner = NULL,
				lease_expires_at = NULL,
				updated_at = now(),
				completed_at = now()
			FROM exhausted
			WHERE j.id = exhausted.id
			RETURNING j.id
		`)
		if err != nil {
			return err
		}
		for _, jobID := range deadLettered {
			if err := appendEventTx(ctx, tx, jobID, "job.dead_lettered", map[string]any{"reason": "lease expired after max attempts"}); err != nil {
				return err
			}
		}
		count = len(deadLettered)
		return nil
	})
	return count, err
}

// pruneBatchSize bounds each retention delete so a large backlog cannot hold
// locks or bloat WAL in one transaction; the remainder goes to later sweeps.
const pruneBatchSize = 5000

func (s *Store) PruneCompleted(ctx context.Context, queue string, olderThan time.Duration) (int, error) {
	if olderThan <= 0 {
		return 0, nil
	}
	count := 0
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		held, err := tryAdvisoryLock(ctx, tx, "workrail:prune:"+queue)
		if err != nil || !held {
			return err
		}
		tag, err := tx.Exec(ctx, `
			DELETE FROM jobs
			WHERE id IN (
				SELECT id FROM jobs
				WHERE queue = $1 AND status IN ('succeeded', 'canceled') AND completed_at < now() - $2::interval
				LIMIT $3
			)
		`, queue, pgInterval(olderThan), pruneBatchSize)
		if err != nil {
			return err
		}
		count = int(tag.RowsAffected())
		return nil
	})
	return count, err
}

func (s *Store) Heartbeat(ctx context.Context, jobID, workerID string, leaseDuration time.Duration) error {
	tag, err := s.db.Exec(ctx, `
		UPDATE jobs
		SET heartbeat_at = now(), lease_expires_at = now() + $3::interval, updated_at = now()
		WHERE id = $1 AND lease_owner = $2 AND status = 'running'
	`, jobID, workerID, pgInterval(leaseDuration))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return engine.ErrInvalidTransition
	}
	return s.appendEvent(ctx, jobID, "job.heartbeat", map[string]any{"worker_id": workerID})
}

func (s *Store) Complete(ctx context.Context, jobID, workerID string, result []byte) error {
	if len(result) == 0 {
		result = []byte(`{}`)
	}
	tag, err := s.db.Exec(ctx, `
		UPDATE jobs
		SET status = 'succeeded', result = $3, lease_owner = NULL, lease_expires_at = NULL,
			updated_at = now(), completed_at = now()
		WHERE id = $1 AND lease_owner = $2 AND status = 'running'
	`, jobID, workerID, json.RawMessage(result))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return engine.ErrInvalidTransition
	}
	return s.appendEvent(ctx, jobID, "job.succeeded", map[string]any{})
}

func (s *Store) Fail(ctx context.Context, jobID, workerID string, cause error) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		var attempt, maxAttempts int
		if err := tx.QueryRow(ctx, `
			SELECT attempt, max_attempts FROM jobs
			WHERE id = $1 AND lease_owner = $2 AND status = 'running'
			FOR UPDATE
		`, jobID, workerID).Scan(&attempt, &maxAttempts); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return engine.ErrInvalidTransition
			}
			return err
		}

		next := engine.NextStatusAfterFailure(attempt, maxAttempts)
		runAfter := time.Now().UTC().Add(engine.Backoff(attempt))
		if next == engine.StatusDeadLetter {
			runAfter = time.Now().UTC()
		}
		if _, err := tx.Exec(ctx, `
			UPDATE jobs
			SET status = $3::job_status, error = $4, run_after = $5, lease_owner = NULL, lease_expires_at = NULL,
				updated_at = now(), completed_at = CASE WHEN $3::job_status = 'dead_letter' THEN now() ELSE NULL END
			WHERE id = $1 AND lease_owner = $2
		`, jobID, workerID, next, cause.Error(), runAfter); err != nil {
			return err
		}
		return appendEventTx(ctx, tx, jobID, "job.failed", map[string]any{"error": cause.Error(), "next_status": next})
	})
}

func (s *Store) Cancel(ctx context.Context, jobID string) error {
	tag, err := s.db.Exec(ctx, `
		UPDATE jobs
		SET status = 'canceled', lease_owner = NULL, lease_expires_at = NULL, updated_at = now(), completed_at = now()
		WHERE id = $1 AND status NOT IN ('succeeded', 'dead_letter', 'canceled')
	`, jobID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return engine.ErrInvalidTransition
	}
	return s.appendEvent(ctx, jobID, "job.canceled", map[string]any{})
}

func (s *Store) RetryDeadLetter(ctx context.Context, jobID string) (engine.Job, error) {
	var job engine.Job
	err := s.db.QueryRow(ctx, `
		UPDATE jobs
		SET status = 'queued', attempt = 0, run_after = now(), lease_owner = NULL, lease_expires_at = NULL,
			heartbeat_at = NULL, result = NULL, error = NULL, updated_at = now(), completed_at = NULL
		WHERE id = $1 AND status = 'dead_letter'
		RETURNING id, queue, workflow_type, payload, status, idempotency_key, attempt, max_attempts, run_after,
			lease_owner, lease_expires_at, heartbeat_at, result, error, trace_id, created_at, updated_at, completed_at
	`, jobID).Scan(jobScanTargets(&job)...)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return job, engine.ErrInvalidTransition
		}
		return job, err
	}
	return job, s.appendEvent(ctx, job.ID, "job.dlq_retried", map[string]any{})
}

func (s *Store) Replay(ctx context.Context, jobID string) (engine.Job, error) {
	var job engine.Job
	err := s.db.QueryRow(ctx, `
		INSERT INTO jobs (queue, workflow_type, payload, max_attempts)
		SELECT queue, workflow_type, payload, max_attempts FROM jobs WHERE id = $1
		RETURNING id, queue, workflow_type, payload, status, idempotency_key, attempt, max_attempts, run_after,
			lease_owner, lease_expires_at, heartbeat_at, result, error, trace_id, created_at, updated_at, completed_at
	`, jobID).Scan(jobScanTargets(&job)...)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return job, engine.ErrNotFound
		}
		return job, err
	}
	return job, s.appendEvent(ctx, job.ID, "job.replayed", map[string]any{"source_job_id": jobID})
}

func (s *Store) Get(ctx context.Context, jobID string) (engine.Job, []engine.Event, error) {
	var job engine.Job
	err := s.db.QueryRow(ctx, `
		SELECT id, queue, workflow_type, payload, status, idempotency_key, attempt, max_attempts, run_after,
			lease_owner, lease_expires_at, heartbeat_at, result, error, trace_id, created_at, updated_at, completed_at
		FROM jobs WHERE id = $1
	`, jobID).Scan(jobScanTargets(&job)...)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return job, nil, engine.ErrNotFound
		}
		return job, nil, err
	}
	events, err := s.events(ctx, jobID)
	return job, events, err
}

func (s *Store) List(ctx context.Context, opts engine.ListOptions) ([]engine.Job, error) {
	if opts.Limit <= 0 || opts.Limit > 100 {
		opts.Limit = 50
	}
	if !engine.IsValidStatus(opts.Status) {
		return nil, engine.ErrInvalidStatus
	}
	rows, err := s.db.Query(ctx, `
		SELECT id, queue, workflow_type, payload, status, idempotency_key, attempt, max_attempts, run_after,
			lease_owner, lease_expires_at, heartbeat_at, result, error, trace_id, created_at, updated_at, completed_at
		FROM jobs
		WHERE ($2 = '' OR queue = $2)
			AND ($3 = '' OR status = $3::job_status)
			AND ($4 = '' OR workflow_type = $4)
		ORDER BY created_at DESC
		LIMIT $1
	`, opts.Limit, opts.Queue, string(opts.Status), opts.WorkflowType)
	if err != nil {
		return nil, err
	}
	return scanJobs(rows)
}

func (s *Store) QueueDepth(ctx context.Context) ([]engine.QueueDepth, error) {
	rows, err := s.db.Query(ctx, `
		SELECT queue, status, count(*)
		FROM jobs
		GROUP BY queue, status
		ORDER BY queue, status
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var depths []engine.QueueDepth
	for rows.Next() {
		var depth engine.QueueDepth
		var status engine.Status
		if err := rows.Scan(&depth.Queue, &status, &depth.Count); err != nil {
			return nil, err
		}
		depth.Status = string(status)
		depths = append(depths, depth)
	}
	return depths, rows.Err()
}

func (s *Store) events(ctx context.Context, jobID string) ([]engine.Event, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, job_id, event_type, details, created_at
		FROM job_events WHERE job_id = $1 ORDER BY id
	`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []engine.Event
	for rows.Next() {
		var event engine.Event
		if err := rows.Scan(&event.ID, &event.JobID, &event.EventType, &event.Details, &event.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *Store) appendEvent(ctx context.Context, jobID, typ string, details any) error {
	return appendEventTx(ctx, s.db, jobID, typ, details)
}

type eventWriter interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func appendEventTx(ctx context.Context, db eventWriter, jobID, typ string, details any) error {
	data, err := json.Marshal(details)
	if err != nil {
		return err
	}
	_, err = db.Exec(ctx, `INSERT INTO job_events (job_id, event_type, details) VALUES ($1, $2, $3)`, jobID, typ, data)
	return err
}

func scanJob(rows pgx.Rows) (engine.Job, error) {
	var job engine.Job
	err := rows.Scan(jobScanTargets(&job)...)
	return job, err
}

// scanJobs closes rows before returning so the connection can run further statements.
func scanJobs(rows pgx.Rows) ([]engine.Job, error) {
	defer rows.Close()
	var jobs []engine.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func queryStrings(ctx context.Context, tx pgx.Tx, sql string, args ...any) ([]string, error) {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func jobScanTargets(job *engine.Job) []any {
	return []any{
		&job.ID, &job.Queue, &job.WorkflowType, &job.Payload, &job.Status, &job.IdempotencyKey, &job.Attempt, &job.MaxAttempts,
		&job.RunAfter, &job.LeaseOwner, &job.LeaseExpiresAt, &job.HeartbeatAt, &job.Result, &job.Error,
		&job.TraceID, &job.CreatedAt, &job.UpdatedAt, &job.CompletedAt,
	}
}

func pgInterval(d time.Duration) string {
	if d <= 0 {
		d = 30 * time.Second
	}
	return fmt.Sprintf("%f seconds", d.Seconds())
}
