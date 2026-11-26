#!/bin/bash
# Runs all schema migrations against smart_inventory DB on first boot.
set -e

echo "Running schema migrations..."

for sql_file in /migrations/001_create_suppliers.sql \
                /migrations/002_create_stores.sql \
                /migrations/003_create_products.sql \
                /migrations/004_create_orders.sql \
                /migrations/005_create_shipments.sql \
                /migrations/006_create_stock_movements.sql \
                /migrations/007_create_historical_sales.sql \
                /migrations/008_create_inventory_demand_stockout_risk.sql \
                /migrations/009_create_streaming_tables.sql \
                /migrations/010_create_analytical_tables.sql \
                /migrations/011_create_quarantine_table.sql; do
    echo "  Applying: $sql_file"
    psql -v ON_ERROR_STOP=1 \
         --username inventory \
         --dbname smart_inventory \
         --file "$sql_file"
done

echo "All migrations complete."
