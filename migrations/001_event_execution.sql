CREATE TABLE IF NOT EXISTS event_outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE,
    event_type varchar(100) NOT NULL,
    aggregate_type varchar(100) NOT NULL,
    aggregate_id varchar(100) NOT NULL,
    correlation_id varchar(100),
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    status varchar(30) NOT NULL DEFAULT 'PENDING'
);

CREATE TABLE IF NOT EXISTS event_execution (
    execution_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE,
    event_type varchar(100) NOT NULL,
    aggregate_type varchar(100) NOT NULL,
    aggregate_id varchar(100) NOT NULL,
    correlation_id varchar(100),
    status varchar(30) NOT NULL DEFAULT 'PENDING',
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz,
    payload jsonb
);

CREATE INDEX IF NOT EXISTS idx_event_execution_status
    ON event_execution (status, processed_at);

CREATE INDEX IF NOT EXISTS idx_event_execution_event_type
    ON event_execution (event_type, aggregate_id);

DELETE FROM event_outbox older
USING event_outbox newer
WHERE older.id > newer.id
    AND older.aggregate_id = newer.aggregate_id
    AND older.event_type = newer.event_type
    AND older.status IN ('PENDING', 'RETRY_SCHEDULED', 'COMPLETED')
    AND newer.status IN ('PENDING', 'RETRY_SCHEDULED', 'COMPLETED');

DROP INDEX IF EXISTS uq_event_outbox_business_event;

CREATE UNIQUE INDEX IF NOT EXISTS uq_event_outbox_business_event
    ON event_outbox (aggregate_type, aggregate_id, event_type, correlation_id)
        WHERE status IN ('PENDING', 'RETRY_SCHEDULED', 'COMPLETED');

CREATE TABLE IF NOT EXISTS event_execution_log (
    execution_id uuid PRIMARY KEY,
    event_id uuid NOT NULL,
    event_type varchar(100) NOT NULL,
    aggregate_type varchar(100) NOT NULL,
    aggregate_id varchar(100) NOT NULL,
    correlation_id uuid,
    status varchar(30) NOT NULL,
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    processing_time_ms integer,
    error_message text
);

DELETE FROM event_execution_log older
USING event_execution_log newer
WHERE older.started_at < newer.started_at
    AND older.event_id = newer.event_id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_event_execution_log_event
        ON event_execution_log (event_id);

CREATE INDEX IF NOT EXISTS idx_event_execution_log_event
    ON event_execution_log (event_id, started_at);

CREATE INDEX IF NOT EXISTS idx_event_execution_log_status
    ON event_execution_log (status, started_at);
