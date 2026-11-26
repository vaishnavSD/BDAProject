-- =============================================================================
-- Migration 001: suppliers
-- Source dataset : reference/suppliers.csv
-- Primary Key    : supplier_id (VARCHAR)
-- Referenced by  : products, orders, shipments, live_orders
-- Data quality   : 2 duplicate rows in CSV → deduplication required before load
--                  reliability_score range 0.0–1.0 (enforced by CHECK)
-- =============================================================================

CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id         VARCHAR(10)     NOT NULL,
    supplier_name       VARCHAR(100)    NOT NULL,
    contact_email       VARCHAR(150),
    country             VARCHAR(60),
    lead_time_days      SMALLINT        CHECK (lead_time_days >= 0),
    reliability_score   NUMERIC(4,2)    CHECK (reliability_score BETWEEN 0.00 AND 1.00),
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,

    -- Audit columns added for all reference tables
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_suppliers PRIMARY KEY (supplier_id)
);

-- Index to support joins from orders and products
CREATE INDEX IF NOT EXISTS idx_suppliers_active
    ON suppliers (is_active)
    WHERE is_active = TRUE;

COMMENT ON TABLE  suppliers                  IS 'Master list of product suppliers. Source: reference/suppliers.csv.';
COMMENT ON COLUMN suppliers.supplier_id      IS 'Business key. Format: SUP001..SUP020.';
COMMENT ON COLUMN suppliers.reliability_score IS 'Fraction 0.0–1.0; derived from historical on-time delivery.';
COMMENT ON COLUMN suppliers.lead_time_days   IS 'Average days from order placement to warehouse receipt.';
