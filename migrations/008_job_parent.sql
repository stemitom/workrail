-- parent_id links child workflows to the parent that spawned them, so the
-- dashboard (and operators) can walk lineage in both directions. Children
-- stand on their own: no cascade, no cancel propagation.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS parent_id uuid REFERENCES jobs(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_parent_id ON jobs (parent_id) WHERE parent_id IS NOT NULL;
