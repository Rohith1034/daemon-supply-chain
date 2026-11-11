CREATE TABLE IF NOT EXISTS silver_ingestion_audit (
    ingestion_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_gcs_path text NOT NULL UNIQUE,
    event_id text,
    event_type text,
    source_checksum char(64) NOT NULL
        CHECK (source_checksum ~ '^[0-9a-f]{64}$'),
    target_table text NOT NULL,
    status text NOT NULL
        CHECK (status IN ('PROCESSING', 'SUCCESS', 'FAILED')),
    attempt_count integer NOT NULL DEFAULT 1
        CHECK (attempt_count > 0),
    rows_read bigint
        CHECK (rows_read IS NULL OR rows_read >= 0),
    rows_written bigint
        CHECK (rows_written IS NULL OR rows_written >= 0),
    started_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at timestamptz,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (
        (status = 'PROCESSING' AND completed_at IS NULL)
        OR
        (status IN ('SUCCESS', 'FAILED') AND completed_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_silver_ingestion_audit_status_updated
    ON silver_ingestion_audit (status, updated_at);

CREATE INDEX IF NOT EXISTS idx_silver_ingestion_audit_event
    ON silver_ingestion_audit (event_id)
    WHERE event_id IS NOT NULL;
