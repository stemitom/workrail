CREATE TABLE IF NOT EXISTS job_steps (
  job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  step_name text NOT NULL,
  result jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (job_id, step_name)
);
