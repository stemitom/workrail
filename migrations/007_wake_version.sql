-- wake_version closes the park race: Claim hands the worker a version, the
-- Signal wake-up bumps it, and Suspend only parks when it still matches. A
-- signal landing between the mailbox check and the park therefore makes the
-- stale Suspend back off, instead of the park burying the wake-up under a nap.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS wake_version int NOT NULL DEFAULT 0;
