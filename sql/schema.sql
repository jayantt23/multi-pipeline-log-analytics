-- NoSQL ETL Phase 1 — Postgres result store
-- Tables: runs (execution metadata), q1/q2/q3 (mandatory query outputs)
-- Re-runnable: CREATE TABLE IF NOT EXISTS keeps existing data on schema reapply.

CREATE TABLE IF NOT EXISTS runs (
    run_id          TEXT PRIMARY KEY,
    pipeline        TEXT        NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'completed',
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ NOT NULL,
    runtime_ms      BIGINT      NOT NULL,
    batch_size      INT         NOT NULL,
    num_batches     INT         NOT NULL,
    avg_batch_size  NUMERIC     NOT NULL,
    total_records   BIGINT      NOT NULL,
    malformed_count BIGINT      NOT NULL
);

-- Idempotent for already-created tables that pre-date this column.
ALTER TABLE runs ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'completed';

CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    run_id        TEXT   NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    log_date      DATE   NOT NULL,
    status_code   INT    NOT NULL,
    request_count BIGINT NOT NULL,
    total_bytes   BIGINT NOT NULL,
    PRIMARY KEY (run_id, log_date, status_code)
);

CREATE TABLE IF NOT EXISTS q2_top_resources (
    run_id              TEXT   NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    rank                INT    NOT NULL,
    resource_path       TEXT   NOT NULL,
    request_count       BIGINT NOT NULL,
    total_bytes         BIGINT NOT NULL,
    distinct_host_count BIGINT NOT NULL,
    PRIMARY KEY (run_id, rank)
);

CREATE TABLE IF NOT EXISTS q3_hourly_errors (
    run_id               TEXT     NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    log_date             DATE     NOT NULL,
    log_hour             INT      NOT NULL,
    error_request_count  BIGINT   NOT NULL,
    total_request_count  BIGINT   NOT NULL,
    error_rate           NUMERIC(20, 6) NOT NULL,
    distinct_error_hosts BIGINT   NOT NULL,
    PRIMARY KEY (run_id, log_date, log_hour)
);

CREATE INDEX IF NOT EXISTS ix_q1_run ON q1_daily_traffic(run_id);
CREATE INDEX IF NOT EXISTS ix_q2_run ON q2_top_resources(run_id);
CREATE INDEX IF NOT EXISTS ix_q3_run ON q3_hourly_errors(run_id);
