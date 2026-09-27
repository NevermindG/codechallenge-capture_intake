CREATE TABLE IF NOT EXISTS captures (
    id              UUID PRIMARY KEY,
    idempotency_key VARCHAR(200) NOT NULL,
    captured_at     TIMESTAMPTZ NOT NULL,
    amount          NUMERIC(38,12) NOT NULL CHECK (amount >= 0),
    currency        CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    image_key       TEXT NOT NULL UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_captures_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT ck_captures_idempotency_key_nonempty CHECK (btrim(idempotency_key) <> ''),
    CONSTRAINT ck_captures_image_key_nonempty CHECK (btrim(image_key) <> '')
);

CREATE TABLE IF NOT EXISTS outbox (
    id              UUID PRIMARY KEY,
    capture_id      UUID NOT NULL UNIQUE REFERENCES captures(id) ON DELETE CASCADE,
    event_type      TEXT NOT NULL,
    delivery_key    TEXT NOT NULL UNIQUE,
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('pending','processing','delivered','dead')),
    attempts        INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until    TIMESTAMPTZ NULL,
    last_error      TEXT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at    TIMESTAMPTZ NULL,
    CONSTRAINT ck_outbox_event_type_nonempty CHECK (btrim(event_type) <> ''),
    CONSTRAINT ck_outbox_delivery_key_nonempty CHECK (btrim(delivery_key) <> '')
);

CREATE INDEX IF NOT EXISTS idx_outbox_claim
    ON outbox (status, next_attempt_at, created_at);

CREATE INDEX IF NOT EXISTS idx_outbox_lock_expiry
    ON outbox (status, locked_until)
    WHERE status = 'processing';
