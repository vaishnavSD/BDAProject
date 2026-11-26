-- =============================================================================
-- Migration 007: historical_sales
-- Source dataset : historical/historical_sales.csv
-- Primary Key    : sale_id (VARCHAR)
-- Foreign Keys   : product_id → products
--                  store_id   → stores
-- Data quality   : 10 rows have quantity_sold = 0 → invalid; these rows must
--                    be quarantined before load; CHECK enforces > 0 here
--                  No NULLs in source
--                  discount_pct range 0.0–0.30 (up to 30% discount)
-- Pipeline role  : BATCH ONLY — full year 2024 historical record
--                  Partitioned by sale_date for Spark efficiency
-- =============================================================================

DO $$ BEGIN
    CREATE TYPE sales_channel_enum AS ENUM
        ('IN_STORE','ONLINE','B2B','WHOLESALE');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE customer_segment_enum AS ENUM
        ('RETAIL','ENTERPRISE','GOVERNMENT','SMB');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS historical_sales (
    sale_id             VARCHAR(10)             NOT NULL,
    product_id          VARCHAR(10)             NOT NULL,
    store_id            VARCHAR(10)             NOT NULL,
    sale_date           DATE                    NOT NULL,
    quantity_sold       INTEGER                 NOT NULL CHECK (quantity_sold > 0),
    unit_price          NUMERIC(10,2)           NOT NULL CHECK (unit_price > 0),
    discount_pct        NUMERIC(5,4)            NOT NULL DEFAULT 0
                                                CHECK (discount_pct BETWEEN 0 AND 1),
    total_revenue       NUMERIC(14,2)           NOT NULL CHECK (total_revenue >= 0),
    channel             sales_channel_enum      NOT NULL,
    customer_segment    customer_segment_enum   NOT NULL,

    created_at          TIMESTAMP               NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_historical_sales      PRIMARY KEY (sale_id),
    CONSTRAINT fk_sales_product         FOREIGN KEY (product_id)
                                        REFERENCES products (product_id) ON UPDATE CASCADE,
    CONSTRAINT fk_sales_store           FOREIGN KEY (store_id)
                                        REFERENCES stores (store_id)   ON UPDATE CASCADE
);

-- Spark batch partitions by month using sale_date
CREATE INDEX IF NOT EXISTS idx_sales_date           ON historical_sales (sale_date DESC);
CREATE INDEX IF NOT EXISTS idx_sales_product_date   ON historical_sales (product_id, sale_date);
CREATE INDEX IF NOT EXISTS idx_sales_store_date     ON historical_sales (store_id, sale_date);
CREATE INDEX IF NOT EXISTS idx_sales_category
    ON historical_sales (product_id);  -- joined with products.category in batch jobs

COMMENT ON TABLE  historical_sales                  IS '2024 full-year sales records. Source: historical/historical_sales.csv.';
COMMENT ON COLUMN historical_sales.sale_id          IS 'Business key. Format: SAL000001..SAL001500.';
COMMENT ON COLUMN historical_sales.discount_pct     IS 'Fraction 0.0–1.0. 10 rows with quantity_sold=0 quarantined before load.';
COMMENT ON COLUMN historical_sales.total_revenue    IS 'Computed: quantity_sold × unit_price × (1 - discount_pct).';
