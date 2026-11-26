-- =============================================================================
-- Migration 011: staging_quarantine
-- Holds rows that FAILED data quality checks during Spark ETL.
-- These rows are NOT inserted into their target tables.
-- A data engineer reviews this table to decide: fix and re-process or discard.
--
-- Rows that land here (from analysis):
--   orders.csv         — 5 rows with status = 'UNKNOWN'
--   orders.csv         — 2 exact duplicate rows
--   suppliers.csv      — 2 exact duplicate rows
--   shipments.csv      — 2 rows with negative freight_cost
--   historical_sales   — 10 rows with quantity_sold = 0
--   stock_movements    — 3 rows with quantity = 0
-- =============================================================================

DO $$ BEGIN
    CREATE TYPE quarantine_reason_enum AS ENUM (
        'DUPLICATE_ROW',
        'INVALID_ENUM_VALUE',
        'NEGATIVE_NUMERIC',
        'ZERO_QUANTITY',
        'NULL_REQUIRED_FIELD',
        'REFERENTIAL_INTEGRITY',
        'BUSINESS_RULE_VIOLATION'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS staging_quarantine (
    quarantine_id       BIGSERIAL,
    source_dataset      VARCHAR(80)             NOT NULL,   -- e.g. 'transactions/orders.csv'
    target_table        VARCHAR(80)             NOT NULL,   -- e.g. 'orders'
    raw_row_json        JSONB                   NOT NULL,   -- full original row as JSON
    reject_reason       quarantine_reason_enum  NOT NULL,
    reject_detail       TEXT,                               -- human-readable explanation
    source_row_number   INTEGER,                            -- original line number in CSV
    spark_job_run_id    VARCHAR(50),
    created_at          TIMESTAMP               NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reviewed_by         VARCHAR(60),
    reviewed_at         TIMESTAMP,
    resolution          VARCHAR(20)
                        CHECK (resolution IN ('FIXED','DISCARDED','PENDING') OR resolution IS NULL),

    CONSTRAINT pk_quarantine PRIMARY KEY (quarantine_id)
);

CREATE INDEX IF NOT EXISTS idx_quarantine_source
    ON staging_quarantine (source_dataset, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_quarantine_pending
    ON staging_quarantine (resolution, created_at DESC)
    WHERE resolution = 'PENDING' OR resolution IS NULL;

COMMENT ON TABLE staging_quarantine IS 'Rows rejected by Spark ETL data quality checks. Review and re-process or discard. Never auto-deleted.';
COMMENT ON COLUMN staging_quarantine.raw_row_json IS 'Full original CSV row stored as JSONB for auditability.';
