CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_jobs_queue_status ON jobs (queue, status);
