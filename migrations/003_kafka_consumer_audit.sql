CREATE TABLE IF NOT EXISTS kafka_consumer_audit (
    dedupe_key text PRIMARY KEY,
    event_id text,
    kafka_topic text NOT NULL,
    kafka_partition integer NOT NULL CHECK (kafka_partition >= 0),
    kafka_offset bigint NOT NULL CHECK (kafka_offset >= 0),
    status text NOT NULL CHECK (status IN ('SUCCESS', 'QUARANTINED')),
    processed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    gcs_path text NOT NULL,
    checksum char(64) NOT NULL,
    payload_sha256 char(64),
    retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    error_message text
);

CREATE INDEX IF NOT EXISTS idx_kafka_consumer_audit_event_status
    ON kafka_consumer_audit (event_id, status)
    WHERE event_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_kafka_consumer_audit_source
    ON kafka_consumer_audit (kafka_topic, kafka_partition, kafka_offset);

CREATE INDEX IF NOT EXISTS idx_kafka_consumer_audit_processed_at
    ON kafka_consumer_audit (processed_at);
