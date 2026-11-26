"""
inventory_turnover.py
─────────────────────────────────────────────────────────────────────────────
Batch Job 4 — Runs monthly on the 1st at 05:00 (Airflow DAG)

What it computes per (product_id, store_id, category) for the report month:
  total_units_sold    = SUM(quantity_sold) from historical_sales
  total_revenue       = SUM(total_revenue) from historical_sales
  avg_inventory       = AVG(closing_stock) from daily snapshots for the month
  turnover_ratio      = total_units_sold / avg_inventory
  days_inventory_held = 30 / turnover_ratio

Interpretation:
  turnover_ratio > 12  → product sells fast (lean inventory, risk of stockout)
  turnover_ratio 4–12  → healthy range
  turnover_ratio < 2   → slow mover (risk of overstock / obsolescence)

Slow movers (turnover_ratio < 2) generate OVERSTOCK INFO alerts.

Arguments:
  --month  YYYY-MM   report month (default: previous month)

Downstream consumer:
  FastAPI GET /reports/inventory-turnover?month=YYYY-MM
  React Dashboard → Turnover Analysis tab
"""

import argparse
import logging
import os
import sys
import uuid
from datetime import date
from calendar import monthrange

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from batch.utils.spark_session import get_batch_session
from batch.utils.jdbc_helpers import (
    read_query,
    overwrite_date_partition,
    write_batch_alerts,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("InventoryTurnover")


def run(report_month: str) -> None:
    """report_month format: 'YYYY-MM'"""
    year, month = int(report_month[:4]), int(report_month[5:7])
    month_start = f"{year}-{month:02d}-01"
    month_end   = f"{year}-{month:02d}-{monthrange(year, month)[1]:02d}"
    run_id = f"turnover-{report_month}-{uuid.uuid4().hex[:8]}"

    logger.info("=" * 60)
    logger.info(
        "InventoryTurnover START  month=%s (%s → %s)  run_id=%s",
        report_month, month_start, month_end, run_id
    )

    spark = get_batch_session("InventoryTurnover")

    # ── 1. Load sales for the month ───────────────────────────────────────────
    sales_sql = f"""
        SELECT hs.product_id,
               hs.store_id,
               p.category,
               SUM(hs.quantity_sold)  AS total_units_sold,
               SUM(hs.total_revenue)  AS total_revenue
        FROM   historical_sales hs
        JOIN   products p ON hs.product_id = p.product_id
        WHERE  hs.sale_date BETWEEN '{month_start}' AND '{month_end}'
          AND  hs.quantity_sold > 0
        GROUP  BY hs.product_id, hs.store_id, p.category
    """
    sales = read_query(spark, sales_sql, "monthly_sales")
    logger.info("Loaded %d product-store-month sales rows.", sales.count())

    # ── 2. Load average inventory from daily snapshots for the month ──────────
    inventory_sql = f"""
        SELECT product_id,
               store_id,
               AVG(closing_stock)::NUMERIC(10,2) AS avg_inventory
        FROM   batch_daily_stock_snapshot
        WHERE  snapshot_date BETWEEN '{month_start}' AND '{month_end}'
        GROUP  BY product_id, store_id
    """
    try:
        avg_inventory = read_query(spark, inventory_sql, "avg_inventory")
        has_snapshots = avg_inventory.count() > 0
    except Exception:
        has_snapshots = False

    # Fallback: use current_stock from risk table if no daily snapshots yet
    if not has_snapshots:
        fallback_sql = f"""
            SELECT DISTINCT ON (product_id, store_id)
                   product_id,
                   store_id,
                   current_stock_units::NUMERIC(10,2) AS avg_inventory
            FROM   inventory_demand_stockout_risk
            ORDER  BY product_id, store_id, snapshot_date DESC
        """
        avg_inventory = read_query(spark, fallback_sql, "avg_inventory_fallback")
        logger.info("Using inventory_demand_stockout_risk as avg_inventory fallback.")

    # ── 3. Join and compute turnover metrics ──────────────────────────────────
    turnover = (
        sales
        .join(avg_inventory, on=["product_id", "store_id"], how="left")
        .withColumn(
            "avg_inventory",
            F.coalesce(F.col("avg_inventory"), F.lit(1.0))  # avoid div-by-zero
        )
        .withColumn(
            "turnover_ratio",
            (F.col("total_units_sold") / F.col("avg_inventory"))
            .cast(DecimalType(8, 4))
        )
        .withColumn(
            "days_inventory_held",
            F.when(
                F.col("turnover_ratio") > 0,
                (F.lit(30.0) / F.col("turnover_ratio")).cast(DecimalType(8, 1))
            ).otherwise(F.lit(None).cast(DecimalType(8, 1)))
        )
        .withColumn("report_month",    F.lit(month_start).cast("date"))
        .withColumn("total_units_sold",F.col("total_units_sold").cast("integer"))
        .withColumn("total_revenue",   F.col("total_revenue").cast(DecimalType(14, 2)))
        .select(
            "product_id", "store_id", "category",
            "report_month", "total_units_sold", "total_revenue",
            "avg_inventory", "turnover_ratio", "days_inventory_held",
        )
    )

    written = overwrite_date_partition(
        df=turnover,
        table="batch_inventory_turnover",
        date_col="report_month",
        partition_date=month_start,
        run_id=run_id,
    )
    logger.info("Wrote %d turnover rows for month %s.", written, report_month)

    # ── 4. Slow-mover alerts (turnover_ratio < 2) ─────────────────────────────
    slow_movers = turnover.filter(
        F.col("turnover_ratio").isNotNull() & (F.col("turnover_ratio") < 2)
    )
    if not slow_movers.rdd.isEmpty():
        alerts = (
            slow_movers
            .withColumn("alert_type", F.lit("OVERSTOCK"))
            .withColumn("severity",   F.lit("INFO"))
            .withColumn(
                "message",
                F.concat(
                    F.lit("[BATCH] SLOW MOVER: Product "), F.col("product_id"),
                    F.lit(" ("), F.col("category"), F.lit(")"),
                    F.lit(" at store "), F.col("store_id"),
                    F.lit(" turnover ratio="), F.col("turnover_ratio").cast("string"),
                    F.lit(" ("), F.col("days_inventory_held").cast("string"),
                    F.lit(" days held) in "), F.lit(report_month),
                )
            )
            .withColumn("supplier_id",    F.lit(None).cast("string"))
            .withColumn("metric_value",   F.col("turnover_ratio").cast(DecimalType(12, 2)))
            .withColumn("threshold_value",F.lit(2).cast(DecimalType(12, 2)))
            .select(
                "alert_type", "severity", "product_id", "store_id",
                "supplier_id", "message", "metric_value", "threshold_value"
            )
        )
        alert_count = write_batch_alerts(alerts, run_id)
        logger.info("Raised %d slow-mover OVERSTOCK alerts.", alert_count)

    logger.info("InventoryTurnover COMPLETE  run_id=%s", run_id)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    today = date.today()
    # Previous month
    if today.month == 1:
        default_month = f"{today.year - 1}-12"
    else:
        default_month = f"{today.year}-{today.month - 1:02d}"
    parser.add_argument(
        "--month",
        default=default_month,
        help="Report month in YYYY-MM format (default: previous month)",
    )
    args = parser.parse_args()
    run(args.month)
