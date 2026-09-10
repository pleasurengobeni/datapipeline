-- Reference DDL — this is also executed automatically by init-user-db.sh
-- on first `docker compose up`. Run manually only when recreating the table.
-- To ADD new columns to an existing installation, run the ALTER TABLE block at the bottom.
CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE IF NOT EXISTS pipeline.etl_metrics (
    id                          BIGSERIAL   PRIMARY KEY,
    dag_id                      TEXT        NOT NULL,
    env                         TEXT        NOT NULL DEFAULT 'dev',
    schema                      TEXT,                           -- source system label used in Dash
    table_name                  TEXT        NOT NULL,

    -- Operational / run metadata
    run_id                      TEXT,                           -- Airflow dag_run.run_id
    run_start                   TIMESTAMP,                      -- dag_run.start_date (scheduler)
    run_end                     TIMESTAMP,                      -- when collect_metrics executed
    run_duration_secs           INTEGER,                        -- wall-clock seconds start→end
    run_status                  TEXT        NOT NULL DEFAULT 'success', -- 'success' | 'failed'
    error_msg                   TEXT,                           -- populated on failure
    batch_size                  INTEGER,                        -- configured rows per batch

    -- Row counts
    rows_loaded_now             INTEGER     NOT NULL DEFAULT 0, -- records written in this run
    target_total_records        INTEGER,
    target_today_records        INTEGER,
    target_max_upload_date      TIMESTAMP,
    target_first_upload_date    TIMESTAMP,
    target_max_incremental      TEXT,
    source_total_records        INTEGER,
    source_max_incremental      TEXT,
    source_loaded_to_max_target INTEGER,
    perc_loaded                 NUMERIC(5,2),
    avg_daily_load              INTEGER,
    weekday_loads               JSONB,
    created_at                  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ─── If the table already exists, add the new columns ────────────────────────
-- (Safe to run repeatedly — IF NOT EXISTS prevents errors)
ALTER TABLE pipeline.etl_metrics
    ADD COLUMN IF NOT EXISTS run_id            TEXT,
    ADD COLUMN IF NOT EXISTS run_start         TIMESTAMP,
    ADD COLUMN IF NOT EXISTS run_end           TIMESTAMP,
    ADD COLUMN IF NOT EXISTS run_duration_secs INTEGER,
    ADD COLUMN IF NOT EXISTS run_status        TEXT NOT NULL DEFAULT 'success',
    ADD COLUMN IF NOT EXISTS error_msg         TEXT,
    ADD COLUMN IF NOT EXISTS batch_size        INTEGER,
    ADD COLUMN IF NOT EXISTS target_count_at_hwm BIGINT,
    ADD COLUMN IF NOT EXISTS source_count_at_hwm BIGINT,
    ADD COLUMN IF NOT EXISTS hwm_check_status    TEXT;
