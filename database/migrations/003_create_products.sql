-- =============================================================================
-- Migration 003: products
-- Source dataset : reference/products.csv
-- Primary Key    : product_id (VARCHAR)
-- Foreign Keys   : supplier_id → suppliers
-- Referenced by  : orders, shipments, stock_movements, historical_sales,
--                  inventory_demand_stockout_risk, live_orders,
--                  live_shipments, live_dispatches
-- Data quality   : 3 rows have NULL unit_cost → stored as NULL
--                  3 product_names are non-unique (same name, different IDs)
--                  → product_id is the true unique identifier
-- =============================================================================

CREATE TABLE IF NOT EXISTS products (
    product_id          VARCHAR(10)     NOT NULL,
    product_name        VARCHAR(100)    NOT NULL,
    category            VARCHAR(50)     NOT NULL,
    sku                 VARCHAR(30)     NOT NULL,
    unit_cost           NUMERIC(10,2),                  -- nullable: 3 rows missing
    unit_price          NUMERIC(10,2)   NOT NULL CHECK (unit_price > 0),
    reorder_point       INTEGER         NOT NULL CHECK (reorder_point >= 0),
    reorder_quantity    INTEGER         NOT NULL CHECK (reorder_quantity > 0),
    weight_kg           NUMERIC(8,2)    CHECK (weight_kg > 0),
    supplier_id         VARCHAR(10)     NOT NULL,
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,

    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_products          PRIMARY KEY (product_id),
    CONSTRAINT uq_products_sku      UNIQUE (sku),
    CONSTRAINT fk_products_supplier FOREIGN KEY (supplier_id)
                                    REFERENCES suppliers (supplier_id)
                                    ON UPDATE CASCADE
                                    ON DELETE RESTRICT
);

-- Category index used heavily by Spark batch aggregations
CREATE INDEX IF NOT EXISTS idx_products_category    ON products (category);
CREATE INDEX IF NOT EXISTS idx_products_supplier    ON products (supplier_id);
CREATE INDEX IF NOT EXISTS idx_products_active      ON products (is_active) WHERE is_active = TRUE;

COMMENT ON TABLE  products                  IS 'Product master data. Source: reference/products.csv.';
COMMENT ON COLUMN products.product_id       IS 'Business key. Format: PRD0001..PRD0100.';
COMMENT ON COLUMN products.sku              IS 'Stock Keeping Unit. Globally unique.';
COMMENT ON COLUMN products.unit_cost        IS 'Purchase cost per unit. 3 rows NULL in source CSV.';
COMMENT ON COLUMN products.reorder_point    IS 'Stock level that triggers a replenishment order.';
COMMENT ON COLUMN products.reorder_quantity IS 'Standard quantity to order upon reorder trigger.';
