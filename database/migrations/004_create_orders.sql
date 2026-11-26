-- =============================================================================
-- Migration 004: orders
-- Source dataset : transactions/orders.csv
-- Primary Key    : order_id (VARCHAR)
-- Foreign Keys   : product_id → products
--                  store_id   → stores
--                  supplier_id → suppliers
-- Referenced by  : shipments
-- Data quality   : 392 rows (78%) have NULL actual_delivery_date → expected
--                    for non-DELIVERED orders; stored as NULL
--                  2 exact duplicate rows in source CSV → dedup before load
--                  5 rows have status = 'UNKNOWN' → invalid enum; must be
--                    quarantined to staging_quarantine or corrected
-- =============================================================================

-- Enum type for order status
DO $$ BEGIN
    CREATE TYPE order_status_enum AS ENUM
        ('PENDING','CONFIRMED','SHIPPED','DELIVERED','CANCELLED');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE order_priority_enum AS ENUM ('LOW','MEDIUM','HIGH','URGENT');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS orders (
    order_id                VARCHAR(10)         NOT NULL,
    product_id              VARCHAR(10)         NOT NULL,
    store_id                VARCHAR(10)         NOT NULL,
    supplier_id             VARCHAR(10)         NOT NULL,
    order_date              DATE                NOT NULL,
    expected_delivery_date  DATE                NOT NULL,
    actual_delivery_date    DATE,                           -- NULL until delivered
    quantity_ordered        INTEGER             NOT NULL CHECK (quantity_ordered > 0),
    unit_price              NUMERIC(10,2)       NOT NULL CHECK (unit_price > 0),
    total_amount            NUMERIC(14,2)       NOT NULL CHECK (total_amount > 0),
    status                  order_status_enum   NOT NULL,
    priority                order_priority_enum NOT NULL,

    created_at              TIMESTAMP           NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP           NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_orders            PRIMARY KEY (order_id),
    CONSTRAINT fk_orders_product    FOREIGN KEY (product_id)
                                    REFERENCES products (product_id) ON UPDATE CASCADE,
    CONSTRAINT fk_orders_store      FOREIGN KEY (store_id)
                                    REFERENCES stores (store_id)   ON UPDATE CASCADE,
    CONSTRAINT fk_orders_supplier   FOREIGN KEY (supplier_id)
                                    REFERENCES suppliers (supplier_id) ON UPDATE CASCADE,
    -- Business rule: actual_delivery_date must be >= order_date
    CONSTRAINT chk_orders_delivery_date
        CHECK (actual_delivery_date IS NULL OR actual_delivery_date >= order_date)
);

CREATE INDEX IF NOT EXISTS idx_orders_product       ON orders (product_id);
CREATE INDEX IF NOT EXISTS idx_orders_store         ON orders (store_id);
CREATE INDEX IF NOT EXISTS idx_orders_supplier      ON orders (supplier_id);
CREATE INDEX IF NOT EXISTS idx_orders_status        ON orders (status);
-- Composite index used by Spark batch reports joining store + date range
CREATE INDEX IF NOT EXISTS idx_orders_store_date    ON orders (store_id, order_date);
-- Partial index for open orders (Spark streaming enrichment lookups)
CREATE INDEX IF NOT EXISTS idx_orders_open
    ON orders (order_id)
    WHERE status IN ('PENDING','CONFIRMED','SHIPPED');

COMMENT ON TABLE  orders                        IS 'Purchase orders. Source: transactions/orders.csv.';
COMMENT ON COLUMN orders.order_id               IS 'Business key. Format: ORD00001..ORD00500.';
COMMENT ON COLUMN orders.actual_delivery_date   IS 'NULL for all statuses except DELIVERED.';
COMMENT ON COLUMN orders.status                 IS '5 rows in source had status=UNKNOWN; those are quarantined.';
