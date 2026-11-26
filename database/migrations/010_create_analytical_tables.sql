-- =============================================================================
-- Migration 010: analytical_tables
-- These tables are written exclusively by Spark batch jobs triggered
-- via Airflow. They are read-only from the FastAPI layer.
--
-- Tables:
--   batch_daily_stock_snapshot    — written by daily_stock_snapshot.py
--   batch_overstock_report        — written by overstock_detection.py
--   batch_supplier_performance    — written by supplier_performance.py
--   batch_inventory_turnover      — written by inventory_turnover.py
--   alerts                        — written by both Spark Streaming and Batch
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 10a. batch_daily_stock_snapshot
-- Computed daily: stock balance per product per store
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS batch_daily_stock_snapshot (
    snapshot_id         SERIAL,
    product_id          VARCHAR(10)     NOT NULL,
    store_id            VARCHAR(10)     NOT NULL,
    snapshot_date       DATE            NOT NULL,
    opening_stock       INTEGER         NOT NULL DEFAULT 0,
    total_inbound       INTEGER         NOT NULL DEFAULT 0,
    total_outbound      INTEGER         NOT NULL DEFAULT 0,
    total_adjustment    INTEGER         NOT NULL DEFAULT 0,
    closing_stock       INTEGER         NOT NULL,       -- opening + inbound - outbound + adjustment
    reorder_point       INTEGER         NOT NULL,
    is_below_reorder    BOOLEAN         NOT NULL DEFAULT FALSE,
    -- Spark job metadata
    spark_job_run_id    VARCHAR(50),
    computed_at         TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_batch_snapshot        PRIMARY KEY (snapshot_id),
    CONSTRAINT uq_batch_snapshot_key    UNIQUE (product_id, store_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_snap_date_product
    ON batch_daily_stock_snapshot (snapshot_date DESC, product_id);
CREATE INDEX IF NOT EXISTS idx_snap_below_reorder
    ON batch_daily_stock_snapshot (snapshot_date, is_below_reorder)
    WHERE is_below_reorder = TRUE;


-- ---------------------------------------------------------------------------
-- 10b. batch_overstock_report
-- Computed daily: products with excess inventory vs. demand velocity
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS batch_overstock_report (
    overstock_id            SERIAL,
    product_id              VARCHAR(10)     NOT NULL,
    store_id                VARCHAR(10)     NOT NULL,
    report_date             DATE            NOT NULL,
    current_stock_units     INTEGER         NOT NULL,
    avg_daily_demand_units  NUMERIC(8,2)    NOT NULL,
    days_of_supply          NUMERIC(8,1),
    reorder_point_units     INTEGER         NOT NULL,
    excess_units            INTEGER,                    -- current_stock - (reorder_point * 2)
    excess_value            NUMERIC(14,2),              -- excess_units * unit_cost
    overstock_severity      VARCHAR(10)
                            CHECK (overstock_severity IN ('LOW','MEDIUM','HIGH')),
    spark_job_run_id        VARCHAR(50),
    computed_at             TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_overstock PRIMARY KEY (overstock_id),
    CONSTRAINT uq_overstock_key UNIQUE (product_id, store_id, report_date)
);

CREATE INDEX IF NOT EXISTS idx_overstock_date
    ON batch_overstock_report (report_date DESC, overstock_severity);


-- ---------------------------------------------------------------------------
-- 10c. batch_supplier_performance
-- Computed weekly: on-time delivery rate and fill rate per supplier
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS batch_supplier_performance (
    perf_id                 SERIAL,
    supplier_id             VARCHAR(10)     NOT NULL,
    report_period_start     DATE            NOT NULL,
    report_period_end       DATE            NOT NULL,
    total_orders            INTEGER         NOT NULL DEFAULT 0,
    on_time_deliveries      INTEGER         NOT NULL DEFAULT 0,
    late_deliveries         INTEGER         NOT NULL DEFAULT 0,
    cancelled_orders        INTEGER         NOT NULL DEFAULT 0,
    on_time_rate            NUMERIC(5,2),               -- on_time / total * 100
    avg_lead_time_days      NUMERIC(6,2),
    total_freight_cost      NUMERIC(14,2),
    fill_rate               NUMERIC(5,2),               -- qty_shipped / qty_ordered * 100
    spark_job_run_id        VARCHAR(50),
    computed_at             TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_supplier_perf PRIMARY KEY (perf_id),
    CONSTRAINT uq_supplier_perf_key
        UNIQUE (supplier_id, report_period_start, report_period_end)
);

CREATE INDEX IF NOT EXISTS idx_supp_perf_supplier
    ON batch_supplier_performance (supplier_id, report_period_start DESC);


-- ---------------------------------------------------------------------------
-- 10d. batch_inventory_turnover
-- Computed monthly: inventory turnover ratio per product per category
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS batch_inventory_turnover (
    turnover_id         SERIAL,
    product_id          VARCHAR(10)     NOT NULL,
    store_id            VARCHAR(10)     NOT NULL,
    category            VARCHAR(50)     NOT NULL,
    report_month        DATE            NOT NULL,       -- first day of month
    total_units_sold    INTEGER         NOT NULL DEFAULT 0,
    total_revenue       NUMERIC(14,2)   NOT NULL DEFAULT 0,
    avg_inventory       NUMERIC(10,2),
    turnover_ratio      NUMERIC(8,4),                  -- units_sold / avg_inventory
    days_inventory_held NUMERIC(8,1),                  -- 30 / turnover_ratio
    spark_job_run_id    VARCHAR(50),
    computed_at         TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_turnover PRIMARY KEY (turnover_id),
    CONSTRAINT uq_turnover_key UNIQUE (product_id, store_id, report_month)
);

CREATE INDEX IF NOT EXISTS idx_turnover_category_month
    ON batch_inventory_turnover (category, report_month DESC);


-- ---------------------------------------------------------------------------
-- 10e. alerts
-- Written by BOTH Spark Streaming (threshold breaches in real-time)
-- and Spark Batch (overstock, demand anomaly alerts once per day).
-- FastAPI WebSocket endpoint polls this table every 5 seconds.
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    CREATE TYPE alert_type_enum AS ENUM
        ('STOCKOUT_RISK','OVERSTOCK','SHIPMENT_DELAY','DEMAND_SPIKE',
         'SUPPLIER_DELAY','LOW_STOCK','REORDER_TRIGGERED');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE alert_severity_enum AS ENUM ('INFO','WARNING','CRITICAL');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE alert_source_enum AS ENUM ('STREAMING','BATCH');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS alerts (
    alert_id            BIGSERIAL,
    alert_type          alert_type_enum     NOT NULL,
    severity            alert_severity_enum NOT NULL,
    source              alert_source_enum   NOT NULL,
    product_id          VARCHAR(10),
    store_id            VARCHAR(10),
    supplier_id         VARCHAR(10),
    message             TEXT                NOT NULL,
    metric_value        NUMERIC(12,2),                  -- e.g. stock level, delay days
    threshold_value     NUMERIC(12,2),                  -- the threshold that was breached
    is_resolved         BOOLEAN             NOT NULL DEFAULT FALSE,
    resolved_at         TIMESTAMP,
    created_at          TIMESTAMP           NOT NULL DEFAULT CURRENT_TIMESTAMP,
    spark_job_run_id    VARCHAR(50),

    CONSTRAINT pk_alerts PRIMARY KEY (alert_id)
);

-- FastAPI WebSocket polling query: unresolved alerts ordered newest first
CREATE INDEX IF NOT EXISTS idx_alerts_unresolved
    ON alerts (created_at DESC)
    WHERE is_resolved = FALSE;

CREATE INDEX IF NOT EXISTS idx_alerts_product
    ON alerts (product_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_severity
    ON alerts (severity, created_at DESC);

COMMENT ON TABLE alerts IS 'Unified alert table. Written by both Spark Streaming (real-time) and Spark Batch (scheduled). Polled by FastAPI WebSocket every 5 seconds.';
