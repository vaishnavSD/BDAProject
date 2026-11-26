-- =============================================================================
-- Migration 009: streaming_tables
-- Three tables that receive output from Spark Structured Streaming jobs.
-- Source CSVs (used only as Kafka simulation input):
--   streaming/live_orders.csv
--   streaming/live_shipments.csv
--   streaming/live_dispatches.csv
--
-- Design decisions:
--   • No FK constraints to orders/shipments: streaming events arrive before
--     batch tables are updated, so strict FK enforcement would cause failures.
--   • event_timestamp is the event time recorded by the source system.
--   • processed_at is set by Spark when writing to PostgreSQL.
--   • Each table has a unique event_id generated from the source file field.
--   • All three tables are append-only (never updated after insert).
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 9a. stream_live_orders
-- Source: streaming/live_orders.csv → Kafka topic: inventory-live-orders
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    CREATE TYPE live_order_status_enum AS ENUM ('PENDING','CONFIRMED');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE source_system_enum AS ENUM ('WEB','MOBILE','B2B_PORTAL','EDI');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS stream_live_orders (
    live_order_id       VARCHAR(10)             NOT NULL,
    product_id          VARCHAR(10)             NOT NULL,
    store_id            VARCHAR(10)             NOT NULL,
    supplier_id         VARCHAR(10)             NOT NULL,
    event_timestamp     TIMESTAMP               NOT NULL,
    quantity_ordered    INTEGER                 NOT NULL CHECK (quantity_ordered > 0),
    unit_price          NUMERIC(10,2)           NOT NULL CHECK (unit_price > 0),
    total_amount        NUMERIC(14,2)           NOT NULL,
    order_status        live_order_status_enum  NOT NULL,
    event_type          VARCHAR(30)             NOT NULL DEFAULT 'ORDER_PLACED',
    source_system       source_system_enum      NOT NULL,

    -- Spark processing metadata
    processed_at        TIMESTAMP               NOT NULL DEFAULT CURRENT_TIMESTAMP,
    kafka_topic         VARCHAR(60),
    kafka_offset        BIGINT,
    spark_batch_id      VARCHAR(30),

    CONSTRAINT pk_stream_live_orders PRIMARY KEY (live_order_id)
);

CREATE INDEX IF NOT EXISTS idx_slo_product_ts
    ON stream_live_orders (product_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_slo_store_ts
    ON stream_live_orders (store_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_slo_processed
    ON stream_live_orders (processed_at DESC);


-- ---------------------------------------------------------------------------
-- 9b. stream_live_shipments
-- Source: streaming/live_shipments.csv → Kafka topic: inventory-live-shipments
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    CREATE TYPE live_shipment_status_enum AS ENUM
        ('DISPATCHED','IN_TRANSIT','OUT_FOR_DELIVERY','DELIVERED','DELAYED');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS stream_live_shipments (
    live_shipment_id    VARCHAR(10)                 NOT NULL,
    -- Soft reference — no FK constraint (see design decisions above)
    shipment_id         VARCHAR(10),
    product_id          VARCHAR(10)                 NOT NULL,
    store_id            VARCHAR(10)                 NOT NULL,
    event_timestamp     TIMESTAMP                   NOT NULL,
    shipment_status     live_shipment_status_enum   NOT NULL,
    current_location    VARCHAR(100),
    estimated_arrival   DATE,
    quantity_in_transit INTEGER                     NOT NULL CHECK (quantity_in_transit > 0),
    event_type          VARCHAR(30)                 NOT NULL DEFAULT 'SHIPMENT_UPDATE',
    carrier             VARCHAR(50),

    processed_at        TIMESTAMP                   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    kafka_topic         VARCHAR(60),
    kafka_offset        BIGINT,
    spark_batch_id      VARCHAR(30),

    CONSTRAINT pk_stream_live_shipments PRIMARY KEY (live_shipment_id)
);

CREATE INDEX IF NOT EXISTS idx_slsh_product_ts
    ON stream_live_shipments (product_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_slsh_status
    ON stream_live_shipments (shipment_status, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_slsh_shipment
    ON stream_live_shipments (shipment_id);


-- ---------------------------------------------------------------------------
-- 9c. stream_live_dispatches
-- Source: streaming/live_dispatches.csv → Kafka topic: inventory-live-dispatches
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    CREATE TYPE dispatch_reason_enum AS ENUM
        ('RESTOCK','TRANSFER','EMERGENCY_REPLENISH','RETURN_TO_SUPPLIER');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE dispatch_status_enum AS ENUM
        ('INITIATED','IN_TRANSIT','COMPLETED','CANCELLED');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE dispatch_priority_enum AS ENUM ('NORMAL','HIGH','URGENT');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS stream_live_dispatches (
    dispatch_id             VARCHAR(10)             NOT NULL,
    product_id              VARCHAR(10)             NOT NULL,
    -- Two store references; both are soft (no FK) for streaming append table
    from_store_id           VARCHAR(10)             NOT NULL,
    to_store_id             VARCHAR(10)             NOT NULL,
    event_timestamp         TIMESTAMP               NOT NULL,
    quantity_dispatched     INTEGER                 NOT NULL CHECK (quantity_dispatched > 0),
    dispatch_reason         dispatch_reason_enum    NOT NULL,
    dispatch_status         dispatch_status_enum    NOT NULL,
    event_type              VARCHAR(30)             NOT NULL DEFAULT 'DISPATCH_EVENT',
    priority                dispatch_priority_enum  NOT NULL,

    processed_at            TIMESTAMP               NOT NULL DEFAULT CURRENT_TIMESTAMP,
    kafka_topic             VARCHAR(60),
    kafka_offset            BIGINT,
    spark_batch_id          VARCHAR(30),

    CONSTRAINT pk_stream_live_dispatches PRIMARY KEY (dispatch_id)
);

CREATE INDEX IF NOT EXISTS idx_sld_product_ts
    ON stream_live_dispatches (product_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sld_from_store
    ON stream_live_dispatches (from_store_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sld_to_store
    ON stream_live_dispatches (to_store_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sld_status
    ON stream_live_dispatches (dispatch_status);

COMMENT ON TABLE stream_live_orders     IS 'Append-only Spark Streaming output from Kafka topic inventory-live-orders.';
COMMENT ON TABLE stream_live_shipments  IS 'Append-only Spark Streaming output from Kafka topic inventory-live-shipments.';
COMMENT ON TABLE stream_live_dispatches IS 'Append-only Spark Streaming output from Kafka topic inventory-live-dispatches.';
