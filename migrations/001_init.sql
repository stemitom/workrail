CREATE EXTENSION IF NOT EXISTS pgcrypto;

DO $$ BEGIN
  CREATE TYPE job_status AS ENUM ('queued', 'running', 'retrying', 'succeeded', 'failed', 'dead_letter', 'canceled');
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS jobs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  queue text NOT NULL DEFAULT 'default',
  workflow_type text NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  status job_status NOT NULL DEFAULT 'queued',
  idempotency_key text UNIQUE,
  attempt int NOT NULL DEFAULT 0,
  max_attempts int NOT NULL DEFAULT 3,
  run_after timestamptz NOT NULL DEFAULT now(),
  lease_owner text,
  lease_expires_at timestamptz,
  heartbeat_at timestamptz,
  result jsonb,
  error text,
  trace_id text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
);

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS queue text NOT NULL DEFAULT 'default';

CREATE TABLE IF NOT EXISTS job_events (
  id bigserial PRIMARY KEY,
  job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  event_type text NOT NULL,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jobs_claimable
  ON jobs (run_after, created_at)
  WHERE status IN ('queued', 'retrying');

CREATE INDEX IF NOT EXISTS idx_jobs_claimable_queue
  ON jobs (queue, run_after, created_at)
  WHERE status IN ('queued', 'retrying');

CREATE INDEX IF NOT EXISTS idx_jobs_lease_expired
  ON jobs (lease_expires_at)
  WHERE status = 'running';

CREATE INDEX IF NOT EXISTS idx_jobs_lease_expired_queue
  ON jobs (queue, lease_expires_at)
  WHERE status = 'running';

CREATE INDEX IF NOT EXISTS idx_job_events_job_id ON job_events (job_id, id);
