CREATE INDEX IF NOT EXISTS idx_jobs_prunable
  ON jobs (completed_at)
  WHERE status IN ('succeeded', 'canceled');
