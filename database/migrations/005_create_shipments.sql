-- =============================================================================
-- Migration 005: shipments
-- Source dataset : transactions/shipments.csv
-- Primary Key    : shipment_id (VARCHAR)
-- Foreign Keys   : order_id    → orders
--                  product_id  → products
--                  store_id    → stores
--                  supplier_id → suppliers
-- Referenced by  : live_shipments (shipment_id soft link — no DB FK because
--                  live events arrive before batch table is fully populated)
-- Data quality   : 152 rows (74%) have NULL actual_arrival → expected for
--                    non-DELIVERED shipments
--                  2 rows have negative freight_cost → invalid; must be
--                    corrected (set to ABS value) or quarantined
-- =============================================================================

DO $$ BEGIN
    CREATE TYPE shipment_status_enum AS ENUM
        ('IN_TRANSIT','DELIVERED','DELAYED','RETURNED');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS shipments (
    shipment_id         VARCHAR(10)             NOT NULL,
    order_id            VARCHAR(10)             NOT NULL,
    product_id          VARCHAR(10)             NOT NULL,
    store_id            VARCHAR(10)             NOT NULL,
    supplier_id         VARCHAR(10)             NOT NULL,
    shipment_date       DATE                    NOT NULL,
    estimated_arrival   DATE                    NOT NULL,
    actual_arrival      TIMESTAMP,                          -- NULL until delivered
    quantity_shipped    INTEGER                 NOT NULL CHECK (quantity_shipped > 0),
    carrier             VARCHAR(50)             NOT NULL,
    tracking_number     VARCHAR(30)             NOT NULL,
    status              shipment_status_enum    NOT NULL,
    freight_cost        NUMERIC(10,2)           CHECK (freight_cost >= 0),  -- 2 rows invalid in source

    created_at          TIMESTAMP               NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP               NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_shipments             PRIMARY KEY (shipment_id),
    CONSTRAINT uq_shipments_tracking    UNIQUE (tracking_number),
    CONSTRAINT fk_shipments_order       FOREIGN KEY (order_id)
                                        REFERENCES orders (order_id)   ON UPDATE CASCADE,
    CONSTRAINT fk_shipments_product     FOREIGN KEY (product_id)
                                        REFERENCES products (product_id) ON UPDATE CASCADE,
    CONSTRAINT fk_shipments_store       FOREIGN KEY (store_id)
                                        REFERENCES stores (store_id)   ON UPDATE CASCADE,
    CONSTRAINT fk_shipments_supplier    FOREIGN KEY (supplier_id)
                                        REFERENCES suppliers (supplier_id) ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_shipments_order      ON shipments (order_id);
CREATE INDEX IF NOT EXISTS idx_shipments_product    ON shipments (product_id);
CREATE INDEX IF NOT EXISTS idx_shipments_store      ON shipments (store_id);
CREATE INDEX IF NOT EXISTS idx_shipments_status     ON shipments (status);
-- Used by Spark streaming to look up in-transit shipments
CREATE INDEX IF NOT EXISTS idx_shipments_in_transit
    ON shipments (shipment_id)
    WHERE status IN ('IN_TRANSIT','DELAYED');

COMMENT ON TABLE  shipments                 IS 'Outbound shipment records. Source: transactions/shipments.csv.';
COMMENT ON COLUMN shipments.shipment_id     IS 'Business key. Format: SHP00001..SHP00205.';
COMMENT ON COLUMN shipments.freight_cost    IS '2 rows have negative values in source; set to NULL or ABS during ETL.';
COMMENT ON COLUMN shipments.actual_arrival  IS 'NULL for all statuses except DELIVERED.';
