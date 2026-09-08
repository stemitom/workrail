ALTER TABLE job_signals ADD COLUMN IF NOT EXISTS idempotency_key text;

-- Duplicate deliveries (webhook retries) are a Noop: the first send wins.
-- NULL/empty keys dedupe nothing, so ordinary sends are unaffected.
CREATE UNIQUE INDEX IF NOT EXISTS idx_job_signals_idempotency
  ON job_signals (job_id, name, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
