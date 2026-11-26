-- =============================================================================
-- Migration 006: stock_movements
-- Source dataset : transactions/stock_movements.csv
-- Primary Key    : movement_id (VARCHAR)
-- Foreign Keys   : product_id → products
--                  store_id   → stores  (nullable — 4 rows missing store_id)
-- Data quality   : 4 rows have NULL store_id → stored as NULL; a default
--                    store ('UNKNOWN') is NOT used to avoid polluting joins
--                  183 rows (18%) have NULL notes → expected; optional field
--                  310 rows have negative quantity → valid for OUTBOUND and
--                    ADJUSTMENT types; enforced by CHECK per movement_type
--                  3 rows have quantity = 0 → invalid; quarantine on load
-- =============================================================================

DO $$ BEGIN
    CREATE TYPE movement_type_enum AS ENUM
        ('INBOUND','OUTBOUND','ADJUSTMENT','TRANSFER','RETURN');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS stock_movements (
    movement_id     VARCHAR(10)             NOT NULL,
    product_id      VARCHAR(10)             NOT NULL,
    store_id        VARCHAR(10),                            -- NULL allowed: 4 rows missing
    movement_type   movement_type_enum      NOT NULL,
    quantity        INTEGER                 NOT NULL CHECK (quantity != 0),
    movement_date   TIMESTAMP               NOT NULL,
    reference_id    VARCHAR(20),
    notes           TEXT,

    created_at      TIMESTAMP               NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_stock_movements       PRIMARY KEY (movement_id),
    CONSTRAINT fk_movements_product     FOREIGN KEY (product_id)
                                        REFERENCES products (product_id) ON UPDATE CASCADE,
    CONSTRAINT fk_movements_store       FOREIGN KEY (store_id)
                                        REFERENCES stores (store_id)   ON UPDATE CASCADE
);

-- Heavily used by Spark batch for running stock balance per product per store
CREATE INDEX IF NOT EXISTS idx_movements_product_store
    ON stock_movements (product_id, store_id);

CREATE INDEX IF NOT EXISTS idx_movements_date
    ON stock_movements (movement_date DESC);

CREATE INDEX IF NOT EXISTS idx_movements_type
    ON stock_movements (movement_type);

-- Partial index for outbound movements (most queried in demand analysis)
CREATE INDEX IF NOT EXISTS idx_movements_outbound
    ON stock_movements (product_id, movement_date)
    WHERE movement_type = 'OUTBOUND';

COMMENT ON TABLE  stock_movements               IS 'All stock in/out/adjustment events. Source: transactions/stock_movements.csv.';
COMMENT ON COLUMN stock_movements.movement_id   IS 'Business key. Format: MOV000001..MOV001000.';
COMMENT ON COLUMN stock_movements.quantity      IS 'Positive for INBOUND/TRANSFER/RETURN; negative for OUTBOUND; signed for ADJUSTMENT.';
COMMENT ON COLUMN stock_movements.store_id      IS '4 rows in source have empty store_id; stored as NULL.';
