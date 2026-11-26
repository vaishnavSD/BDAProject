-- =============================================================================
-- Master migration runner
-- Execute this file against the target PostgreSQL database to build the
-- entire schema in dependency order.
--
-- Usage:
--   psql -U postgres -d smart_inventory -f 000_run_all_migrations.sql
--
-- Or run individually in this exact order:
--   001 → suppliers         (no dependencies)
--   002 → stores            (no dependencies)
--   003 → products          (depends on: suppliers)
--   004 → orders            (depends on: products, stores, suppliers)
--   005 → shipments         (depends on: orders, products, stores, suppliers)
--   006 → stock_movements   (depends on: products, stores)
--   007 → historical_sales  (depends on: products, stores)
--   008 → inventory_demand_stockout_risk (depends on: products, stores)
--   009 → streaming tables  (append-only; no FK constraints)
--   010 → analytical tables (written by Spark batch jobs)
--   011 → quarantine table  (data quality sink)
-- =============================================================================

\i 001_create_suppliers.sql
\i 002_create_stores.sql
\i 003_create_products.sql
\i 004_create_orders.sql
\i 005_create_shipments.sql
\i 006_create_stock_movements.sql
\i 007_create_historical_sales.sql
\i 008_create_inventory_demand_stockout_risk.sql
\i 009_create_streaming_tables.sql
\i 010_create_analytical_tables.sql
\i 011_create_quarantine_table.sql
