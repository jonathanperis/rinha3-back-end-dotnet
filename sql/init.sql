CREATE TABLE IF NOT EXISTS processed_payments (
    correlation_id UUID PRIMARY KEY,
    amount NUMERIC NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    processor SMALLINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_processed_payments_requested_at ON processed_payments (requested_at);
CREATE INDEX IF NOT EXISTS ix_processed_payments_processor ON processed_payments (processor);