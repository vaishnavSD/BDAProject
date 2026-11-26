-- =============================================================================
-- Seed: Verification query set
-- Run after migrations + CSV load to confirm data loaded correctly.
-- =============================================================================

-- Row count verification
SELECT 'suppliers'                       AS table_name, COUNT(*) AS row_count FROM suppliers
UNION ALL SELECT 'stores',               COUNT(*) FROM stores
UNION ALL SELECT 'products',             COUNT(*) FROM products
UNION ALL SELECT 'orders',               COUNT(*) FROM orders
UNION ALL SELECT 'shipments',            COUNT(*) FROM shipments
UNION ALL SELECT 'stock_movements',      COUNT(*) FROM stock_movements
UNION ALL SELECT 'historical_sales',     COUNT(*) FROM historical_sales
UNION ALL SELECT 'inventory_demand_stockout_risk', COUNT(*) FROM inventory_demand_stockout_risk
ORDER BY table_name;

-- FK integrity spot-checks
SELECT COUNT(*) AS orphan_orders_product
FROM orders o
LEFT JOIN products p ON o.product_id = p.product_id
WHERE p.product_id IS NULL;

SELECT COUNT(*) AS orphan_shipments_order
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Null checks on critical columns
SELECT COUNT(*) AS products_missing_unit_cost
FROM products WHERE unit_cost IS NULL;

SELECT COUNT(*) AS movements_missing_store
FROM stock_movements WHERE store_id IS NULL;

-- Business rule checks
SELECT COUNT(*) AS sales_zero_quantity
FROM historical_sales WHERE quantity_sold = 0;

SELECT COUNT(*) AS negative_freight
FROM shipments WHERE freight_cost < 0;

-- Current stockout risk summary
SELECT
    s.store_name,
    COUNT(*) AS products_at_stockout_risk
FROM inventory_demand_stockout_risk r
JOIN stores s ON r.store_id = s.store_id
WHERE r.stockout_risk_flag = TRUE
  AND r.snapshot_date = (SELECT MAX(snapshot_date) FROM inventory_demand_stockout_risk)
GROUP BY s.store_name
ORDER BY products_at_stockout_risk DESC;
