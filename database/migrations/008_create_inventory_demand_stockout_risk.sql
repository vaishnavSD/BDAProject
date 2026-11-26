-- =============================================================================
-- Migration 008: inventory_demand_stockout_risk
-- Source dataset : historical/inventory_demand_stockout_risk.csv
-- Primary Key    : risk_id (VARCHAR)
-- Foreign Keys   : product_id → products
--                  store_id   → stores
-- Natural key    : (product_id, store_id, snapshot_date) — one snapshot
--                  per product per store per day
-- Data quality   : 5 rows have NULL days_of_stock_remaining → stored NULL;
--                  typically caused by avg_daily_demand_units = 0
--                  current_stock_units can be 0 (genuine stockout)
-- Pipeline role  : BATCH ONLY — periodic snapshots (Oct–Dec 2024)
--                  Written by Spark daily_stock_snapshot batch job
-- =============================================================================

CREATE TABLE IF NOT EXISTS inventory_demand_stockout_risk (
    risk_id                     VARCHAR(10)     NOT NULL,
    product_id                  VARCHAR(10)     NOT NULL,
    store_id                    VARCHAR(10)     NOT NULL,
    snapshot_date               DATE            NOT NULL,
    current_stock_units         INTEGER         NOT NULL CHECK (current_stock_units >= 0),
    avg_daily_demand_units      NUMERIC(8,2)    NOT NULL CHECK (avg_daily_demand_units >= 0),
    days_of_stock_remaining     NUMERIC(8,1),               -- NULL when demand is 0
    reorder_point_units         INTEGER         NOT NULL CHECK (reorder_point_units >= 0),
    stockout_risk_flag          BOOLEAN         NOT NULL DEFAULT FALSE,
    overstock_flag              BOOLEAN         NOT NULL DEFAULT FALSE,
    last_restocked_date         DATE,

    created_at                  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_inventory_risk            PRIMARY KEY (risk_id),
    -- One snapshot record per product-store-date combination
    CONSTRAINT uq_inventory_risk_snapshot   UNIQUE (product_id, store_id, snapshot_date),
    CONSTRAINT fk_inv_risk_product          FOREIGN KEY (product_id)
                                            REFERENCES products (product_id) ON UPDATE CASCADE,
    CONSTRAINT fk_inv_risk_store            FOREIGN KEY (store_id)
                                            REFERENCES stores (store_id)   ON UPDATE CASCADE
);

-- Primary query pattern: "which products are at risk in which store today?"
CREATE INDEX IF NOT EXISTS idx_inv_risk_stockout
    ON inventory_demand_stockout_risk (snapshot_date, stockout_risk_flag)
    WHERE stockout_risk_flag = TRUE;

CREATE INDEX IF NOT EXISTS idx_inv_risk_overstock
    ON inventory_demand_stockout_risk (snapshot_date, overstock_flag)
    WHERE overstock_flag = TRUE;

CREATE INDEX IF NOT EXISTS idx_inv_risk_product_store
    ON inventory_demand_stockout_risk (product_id, store_id, snapshot_date DESC);

COMMENT ON TABLE  inventory_demand_stockout_risk IS 'Daily inventory health snapshots. Source: historical/inventory_demand_stockout_risk.csv. Also written by Spark batch job daily_stock_snapshot.py.';
COMMENT ON COLUMN inventory_demand_stockout_risk.days_of_stock_remaining IS 'NULL when avg_daily_demand_units = 0.';
COMMENT ON COLUMN inventory_demand_stockout_risk.stockout_risk_flag       IS 'TRUE when current_stock_units < reorder_point_units.';
COMMENT ON COLUMN inventory_demand_stockout_risk.overstock_flag           IS 'TRUE when current_stock_units > reorder_point_units × 5.';
