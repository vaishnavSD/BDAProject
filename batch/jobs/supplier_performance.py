"""
supplier_performance.py
─────────────────────────────────────────────────────────────────────────────
Batch Job 3 — Runs weekly on Monday at 04:00 (Airflow DAG)

What it computes for each supplier over the reporting period:
  total_orders        = COUNT of orders placed
  on_time_deliveries  = orders delivered on or before expected_delivery_date
  late_deliveries     = DELIVERED orders that exceeded expected_delivery_date
  cancelled_orders    = orders with status = CANCELLED
  on_time_rate        = on_time_deliveries / total_orders * 100
  avg_lead_time_days  = AVG(actual_delivery_date - order_date) for DELIVERED
  total_freight_cost  = SUM(freight_cost) from shipments for the period
  fill_rate           = SUM(quantity_shipped) / SUM(quantity_ordered) * 100

  Raises SUPPLIER_DELAY alerts for suppliers with on_time_rate < 70%.

Arguments:
  --start  YYYY-MM-DD   period start (default: 7 days ago)
  --end    YYYY-MM-DD   period end   (default: yesterday)

Downstream consumer:
  FastAPI GET /reports/supplier-performance
  React Dashboard → Supplier Performance tab
"""

import argparse
import logging
import os
import sys
import uuid
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from batch.utils.spark_session import get_batch_session
from batch.utils.jdbc_helpers import (
    read_query,
    append_to_postgres,
    write_batch_alerts,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("SupplierPerformance")


def run(period_start: str, period_end: str) -> None:
    run_id = f"supplier-perf-{period_start}-{period_end}-{uuid.uuid4().hex[:8]}"
    logger.info("=" * 60)
    logger.info(
        "SupplierPerformance START  period=%s → %s  run_id=%s",
        period_start, period_end, run_id
    )

    spark = get_batch_session("SupplierPerformance")

    # ── 1. Load orders for the period ─────────────────────────────────────────
    orders_sql = f"""
        SELECT o.order_id,
               o.supplier_id,
               o.product_id,
               o.order_date,
               o.expected_delivery_date,
               o.actual_delivery_date,
               o.quantity_ordered,
               o.status
        FROM   orders o
        WHERE  o.order_date BETWEEN '{period_start}' AND '{period_end}'
    """
    orders = read_query(spark, orders_sql, "orders_period")
    logger.info("Loaded %d orders for period.", orders.count())

    # ── 2. Compute order-level metrics ────────────────────────────────────────
    orders_metrics = (
        orders
        .withColumn(
            "is_delivered",
            F.col("status") == "DELIVERED"
        )
        .withColumn(
            "is_cancelled",
            F.col("status") == "CANCELLED"
        )
        .withColumn(
            "is_on_time",
            (F.col("status") == "DELIVERED") &
            (F.col("actual_delivery_date") <= F.col("expected_delivery_date"))
        )
        .withColumn(
            "is_late",
            (F.col("status") == "DELIVERED") &
            (F.col("actual_delivery_date") > F.col("expected_delivery_date"))
        )
        .withColumn(
            "lead_time_days",
            F.when(
                F.col("status") == "DELIVERED",
                F.datediff(
                    F.col("actual_delivery_date"),
                    F.col("order_date")
                )
            ).otherwise(F.lit(None).cast("integer"))
        )
    )

    order_agg = (
        orders_metrics
        .groupBy("supplier_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum(F.col("is_on_time").cast("integer")).alias("on_time_deliveries"),
            F.sum(F.col("is_late").cast("integer")).alias("late_deliveries"),
            F.sum(F.col("is_cancelled").cast("integer")).alias("cancelled_orders"),
            F.avg("lead_time_days").alias("avg_lead_time_days_raw"),
        )
    )

    # ── 3. Load shipment freight costs for the period ─────────────────────────
    shipments_sql = f"""
        SELECT supplier_id,
               SUM(CASE WHEN freight_cost > 0 THEN freight_cost ELSE 0 END) AS total_freight_cost,
               SUM(quantity_shipped)  AS total_shipped,
               COUNT(shipment_id)     AS total_shipments
        FROM   shipments
        WHERE  shipment_date BETWEEN '{period_start}' AND '{period_end}'
        GROUP  BY supplier_id
    """
    shipments_agg = read_query(spark, shipments_sql, "shipments_agg")

    # ── 4. Load total quantity ordered per supplier for fill rate ─────────────
    qty_sql = f"""
        SELECT supplier_id,
               SUM(quantity_ordered) AS total_ordered
        FROM   orders
        WHERE  order_date BETWEEN '{period_start}' AND '{period_end}'
        GROUP  BY supplier_id
    """
    qty_agg = read_query(spark, qty_sql, "qty_agg")

    # ── 5. Load supplier names ────────────────────────────────────────────────
    suppliers_sql = "SELECT supplier_id, supplier_name FROM suppliers"
    suppliers = read_query(spark, suppliers_sql, "suppliers")

    # ── 6. Join all metrics together ──────────────────────────────────────────
    performance = (
        order_agg
        .join(shipments_agg, on="supplier_id", how="left")
        .join(qty_agg,       on="supplier_id", how="left")
        .join(suppliers,     on="supplier_id", how="left")
        .withColumn(
            "on_time_rate",
            F.when(
                F.col("total_orders") > 0,
                (F.col("on_time_deliveries") / F.col("total_orders") * 100)
                .cast(DecimalType(5, 2))
            ).otherwise(F.lit(None).cast(DecimalType(5, 2)))
        )
        .withColumn(
            "avg_lead_time_days",
            F.col("avg_lead_time_days_raw").cast(DecimalType(6, 2))
        )
        .withColumn(
            "fill_rate",
            F.when(
                F.col("total_ordered") > 0,
                (F.col("total_shipped") / F.col("total_ordered") * 100)
                .cast(DecimalType(5, 2))
            ).otherwise(F.lit(None).cast(DecimalType(5, 2)))
        )
        .withColumn(
            "total_freight_cost",
            F.coalesce(F.col("total_freight_cost"), F.lit(0)).cast(DecimalType(14, 2))
        )
        .withColumn("report_period_start", F.lit(period_start).cast("date"))
        .withColumn("report_period_end",   F.lit(period_end).cast("date"))
        .select(
            "supplier_id",
            "report_period_start",
            "report_period_end",
            "total_orders",
            "on_time_deliveries",
            "late_deliveries",
            "cancelled_orders",
            "on_time_rate",
            "avg_lead_time_days",
            "total_freight_cost",
            "fill_rate",
        )
        .dropDuplicates(["supplier_id", "report_period_start", "report_period_end"])
    )

    written = append_to_postgres(performance, "batch_supplier_performance", run_id)
    logger.info("Wrote %d supplier performance rows.", written)

    # ── 7. SUPPLIER_DELAY alerts for poor performers ──────────────────────────
    poor_performers = performance.filter(
        F.col("on_time_rate").isNotNull() &
        (F.col("on_time_rate") < 70)
    )
    if not poor_performers.rdd.isEmpty():
        alerts = (
            poor_performers
            .withColumn("alert_type", F.lit("SUPPLIER_DELAY"))
            .withColumn(
                "severity",
                F.when(F.col("on_time_rate") < 50, F.lit("CRITICAL"))
                 .otherwise(F.lit("WARNING"))
            )
            .withColumn(
                "message",
                F.concat(
                    F.lit("[BATCH] LOW SUPPLIER ON-TIME RATE: Supplier "),
                    F.col("supplier_id"),
                    F.lit(" achieved "),
                    F.col("on_time_rate").cast("string"),
                    F.lit("% on-time delivery for period "),
                    F.col("report_period_start").cast("string"),
                    F.lit(" → "),
                    F.col("report_period_end").cast("string"),
                )
            )
            .withColumn("product_id",    F.lit(None).cast("string"))
            .withColumn("store_id",      F.lit(None).cast("string"))
            .withColumn("metric_value",  F.col("on_time_rate").cast(DecimalType(12, 2)))
            .withColumn("threshold_value", F.lit(70).cast(DecimalType(12, 2)))
            .select(
                "alert_type", "severity", "product_id", "store_id",
                "supplier_id", "message", "metric_value", "threshold_value"
            )
        )
        alert_count = write_batch_alerts(alerts, run_id)
        logger.info("Raised %d SUPPLIER_DELAY alerts.", alert_count)

    logger.info("SupplierPerformance COMPLETE  run_id=%s", run_id)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    week_ago  = (date.today() - timedelta(days=7)).isoformat()
    parser.add_argument("--start", default=week_ago,  help="Period start YYYY-MM-DD")
    parser.add_argument("--end",   default=yesterday, help="Period end YYYY-MM-DD")
    args = parser.parse_args()
    run(args.start, args.end)
