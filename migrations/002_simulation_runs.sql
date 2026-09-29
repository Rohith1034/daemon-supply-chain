CREATE TABLE IF NOT EXISTS simulation_runs (
    simulation_id varchar(128) PRIMARY KEY,
    correlation_id uuid NOT NULL,
    flow varchar(32) NOT NULL CHECK (flow IN ('inbound', 'outbound', 'transportation')),
    simulation_timestamp timestamptz NOT NULL,
    status varchar(16) NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    retryable boolean NOT NULL DEFAULT false,
    runner_result jsonb,
    response jsonb NOT NULL,
    error text,
    started_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_simulation_runs_flow_correlation
    ON simulation_runs (flow, correlation_id);

CREATE INDEX IF NOT EXISTS idx_simulation_runs_status_started
    ON simulation_runs (status, started_at);