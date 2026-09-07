-- Schema for the payments example. It lives beside Workrail's own tables in
-- the same database: the demo is an application that embeds Workrail, not a
-- fork of it, so its money lives in its own tables.

CREATE TABLE IF NOT EXISTS accounts (
    id             text PRIMARY KEY,
    name           text        NOT NULL,
    balance_cents  bigint      NOT NULL CHECK (balance_cents >= 0),
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS payouts (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id      text        NOT NULL REFERENCES accounts (id),
    amount_cents    bigint      NOT NULL CHECK (amount_cents > 0),
    destination     jsonb       NOT NULL,
    status          text        NOT NULL DEFAULT 'pending',
    bank_ref        text,
    failure_reason  text,
    job_id          uuid,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_payouts_created_at ON payouts (created_at DESC);

-- One row per (payout, kind). The unique constraint is what makes the ledger
-- steps safe to re-run: a replayed hold conflicts instead of double-debiting.
CREATE TABLE IF NOT EXISTS ledger_entries (
    id           bigserial PRIMARY KEY,
    account_id   text        NOT NULL REFERENCES accounts (id),
    payout_id    uuid        NOT NULL REFERENCES payouts (id),
    kind         text        NOT NULL CHECK (kind IN ('hold', 'post', 'release')),
    amount_cents bigint      NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (payout_id, kind)
);

-- The fake bank rail's own store. `attempts` is what makes injected transient
-- failures deterministic, and `settle_after` is the settlement lag.
CREATE TABLE IF NOT EXISTS bank_transfers (
    idempotency_key text PRIMARY KEY,
    ref             text        NOT NULL,
    status          text        NOT NULL DEFAULT 'pending',
    amount_cents    bigint      NOT NULL,
    attempts        int         NOT NULL DEFAULT 0,
    settle_after    timestamptz NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS notifications (
    id         bigserial PRIMARY KEY,
    payout_id  uuid        NOT NULL REFERENCES payouts (id),
    channel    text        NOT NULL,
    body       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Submission attempts per idempotency key, counted separately from the
-- transfer itself so a failed submission still increments.
CREATE TABLE IF NOT EXISTS bank_attempts (
    idempotency_key text PRIMARY KEY,
    attempts        int NOT NULL DEFAULT 0
);
