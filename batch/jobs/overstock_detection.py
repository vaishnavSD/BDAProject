"""
overstock_detection.py
─────────────────────────────────────────────────────────────────────────────
Batch Job 2 — Runs daily at 03:00 (after daily_stock_snapshot completes)

What it computes:
  For every (product_id, store_id) combination where:
    closing_stock > reorder_point * 2   (more than 2× safety buffer)

  Computes:
    avg_daily_demand   = total units sold in the past 30 days / 30
    days_of_supply     = closing_stock / avg_daily_demand
    excess_units       = closing_stock - (reorder_point * 2)
    excess_value       = excess_units * unit_cost
    overstock_severity = LOW (2–4× reorder), MEDIUM (4–8×), HIGH (>8×)

  Results written to batch_overstock_report.
  HIGH severity rows also generate OVERSTOCK batch alerts.

Idempotency: deletes existing rows for report_date before writing.

Arguments:
  --date  YYYY-MM-DD   report date (default: yesterday)

Downstream consumer:
  FastAPI GET /reports/overstock?date=YYYY-MM-DD
  React Dashboard → Overstock tab
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
    overwrite_date_partition,
    write_batch_alerts,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("OverstockDetection")


def run(report_date: str) -> None:
    run_id = f"overstock-{report_date}-{uuid.uuid4().hex[:8]}"
    logger.info("=" * 60)
    logger.info("OverstockDetection START  date=%s  run_id=%s", report_date, run_id)

    spark = get_batch_session("OverstockDetection")

    # ── 1. Load latest stock snapshot for report_date ─────────────────────────
    snapshot_sql = f"""
        SELECT s.product_id,
               s.store_id,
               s.closing_stock,
               s.reorder_point,
               p.unit_cost
        FROM   batch_daily_stock_snapshot s
        JOIN   products p ON s.product_id = p.product_id
        WHERE  s.snapshot_date = '{report_date}'
          AND  s.closing_stock > s.reorder_point * 2
    """
    snapshot = read_query(spark, snapshot_sql, "overstock_candidates")
    candidate_count = snapshot.count()
    logger.info("Found %d overstock candidates on %s", candidate_count, report_date)

    if candidate_count == 0:
        logger.info("No overstock candidates — job complete.")
        spark.stop()
        return

    # ── 2. Compute avg daily demand over past 30 days ─────────────────────────
    past_30_start = (date.fromisoformat(report_date) - timedelta(days=30)).isoformat()
    demand_sql = f"""
        SELECT product_id,
               store_id,
               SUM(ABS(quantity)) / 30.0 AS avg_daily_demand
        FROM   stock_movements
        WHERE  movement_type = 'OUTBOUND'
          AND  movement_date::date BETWEEN '{past_30_start}' AND '{report_date}'
          AND  store_id IS NOT NULL
        GROUP  BY product_id, store_id
    """
    demand = read_query(spark, demand_sql, "avg_demand")

    # ── 3. Join snapshot with demand ──────────────────────────────────────────
    enriched = (
        snapshot
        .join(demand, on=["product_id", "store_id"], how="left")
        .withColumn(
            "avg_daily_demand_units",
            F.coalesce(F.col("avg_daily_demand"), F.lit(0.01))   # avoid div-by-zero
        )
        .drop("avg_daily_demand")
        .withColumn(
            "days_of_supply",
            (F.col("closing_stock") / F.col("avg_daily_demand_units")).cast(DecimalType(8, 1))
        )
        .withColumn(
            "excess_units",
            (F.col("closing_stock") - F.col("reorder_point") * 2).cast("integer")
        )
        .withColumn(
            "excess_value",
            F.when(
                F.col("unit_cost").isNotNull(),
                (F.col("excess_units") * F.col("unit_cost")).cast(DecimalType(14, 2))
            ).otherwise(F.lit(None).cast(DecimalType(14, 2)))
        )
        # Severity: ratio of closing_stock to reorder_point
        .withColumn(
            "_stock_ratio",
            F.col("closing_stock") / F.col("reorder_point")
        )
        .withColumn(
            "overstock_severity",
            F.when(F.col("_stock_ratio") > 8,  F.lit("HIGH"))
             .when(F.col("_stock_ratio") > 4,  F.lit("MEDIUM"))
             .otherwise(                        F.lit("LOW"))
        )
        .drop("_stock_ratio")
        .withColumn("current_stock_units",    F.col("closing_stock").cast("integer"))
        .withColumn("reorder_point_units",    F.col("reorder_point").cast("integer"))
        .withColumn("report_date",            F.lit(report_date).cast("date"))
        .select(
            "product_id", "store_id", "report_date",
            "current_stock_units", "avg_daily_demand_units", "days_of_supply",
            "reorder_point_units", "excess_units", "excess_value",
            "overstock_severity",
        )
    )

    # ── 4. Write report (idempotent) ──────────────────────────────────────────
    written = overwrite_date_partition(
        df=enriched,
        table="batch_overstock_report",
        date_col="report_date",
        partition_date=report_date,
        run_id=run_id,
    )
    logger.info("Wrote %d rows to batch_overstock_report.", written)

    # ── 5. OVERSTOCK alerts for HIGH severity rows ────────────────────────────
    high_severity = enriched.filter(F.col("overstock_severity") == "HIGH")
    if not high_severity.rdd.isEmpty():
        alerts = (
            high_severity
            .withColumn("alert_type", F.lit("OVERSTOCK"))
            .withColumn("severity",   F.lit("WARNING"))
            .withColumn(
                "message",
                F.concat(
                    F.lit("[BATCH] HIGH OVERSTOCK: Product "), F.col("product_id"),
                    F.lit(" at store "), F.col("store_id"),
                    F.lit(" has "), F.col("current_stock_units").cast("string"),
                    F.lit(" units ("), F.col("days_of_supply").cast("string"),
                    F.lit(" days of supply). Excess value: ₹"),
                    F.coalesce(F.col("excess_value").cast("string"), F.lit("N/A")),
                )
            )
            .withColumn("supplier_id",    F.lit(None).cast("string"))
            .withColumn("metric_value",   F.col("current_stock_units").cast(DecimalType(12, 2)))
            .withColumn("threshold_value",F.col("reorder_point_units").cast(DecimalType(12, 2)))
            .select(
                "alert_type", "severity", "product_id", "store_id",
                "supplier_id", "message", "metric_value", "threshold_value"
            )
        )
        alert_count = write_batch_alerts(alerts, run_id)
        logger.info("Raised %d OVERSTOCK batch alerts.", alert_count)

    logger.info("OverstockDetection COMPLETE  run_id=%s", run_id)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default=(date.today() - timedelta(days=1)).isoformat(),
        help="Report date YYYY-MM-DD (default: yesterday)",
    )
    args = parser.parse_args()
    run(args.date)
