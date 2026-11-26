-- =============================================================================
-- Migration 002: stores
-- Source dataset : reference/stores.csv
-- Primary Key    : store_id (VARCHAR)
-- Referenced by  : orders, shipments, stock_movements, historical_sales,
--                  inventory_demand_stockout_risk, live_orders,
--                  live_shipments, live_dispatches (from + to)
-- Data quality   : 1 row has NULL region → stored as NULL, treated as
--                  'UNKNOWN' in Spark transformations
-- =============================================================================

CREATE TABLE IF NOT EXISTS stores (
    store_id                    VARCHAR(10)     NOT NULL,
    store_name                  VARCHAR(100)    NOT NULL,
    region                      VARCHAR(30),            -- nullable: 1 row missing
    city                        VARCHAR(60)     NOT NULL,
    warehouse_capacity_units    INTEGER         CHECK (warehouse_capacity_units > 0),
    is_active                   BOOLEAN         NOT NULL DEFAULT TRUE,

    created_at                  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_stores PRIMARY KEY (store_id)
);

-- Region index used by Spark batch partitioning
CREATE INDEX IF NOT EXISTS idx_stores_region  ON stores (region);
CREATE INDEX IF NOT EXISTS idx_stores_active  ON stores (is_active) WHERE is_active = TRUE;

COMMENT ON TABLE  stores                            IS 'Warehouse / store locations. Source: reference/stores.csv.';
COMMENT ON COLUMN stores.store_id                   IS 'Business key. Format: STR001..STR015.';
COMMENT ON COLUMN stores.region                     IS 'Geographical region. One row in source CSV has empty region.';
COMMENT ON COLUMN stores.warehouse_capacity_units   IS 'Maximum units the warehouse can hold.';
