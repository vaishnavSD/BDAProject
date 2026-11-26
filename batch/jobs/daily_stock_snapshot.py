"""
daily_stock_snapshot.py
─────────────────────────────────────────────────────────────────────────────
Batch Job 1 — Runs daily at 02:00 (triggered by Airflow DAG)

What it computes:
  For every (product_id, store_id) combination that had any stock movement
  on the target date, computes:

    opening_stock   = closing_stock from the previous day's snapshot
                      (or 0 if no prior snapshot exists)
    total_inbound   = SUM of positive movements (INBOUND, TRANSFER in, RETURN)
    total_outbound  = SUM of ABS of negative movements (OUTBOUND, TRANSFER out)
    total_adjustment= SUM of ADJUSTMENT movements (can be positive or negative)
    closing_stock   = opening + inbound - outbound + adjustment
    reorder_point   = from products table
    is_below_reorder= closing_stock < reorder_point

  Results are written to batch_daily_stock_snapshot.
  Rows that breach reorder_point also generate LOW_STOCK batch alerts.

Idempotency: the job deletes any existing rows for snapshot_date before
  writing, so re-running for the same date is safe.

Arguments:
  --date  YYYY-MM-DD   target date to compute (default: yesterday)

Spark-submit:
  spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/bitnami/spark/jars/postgresql-42.7.3.jar \
    /opt/spark-apps/batch/jobs/daily_stock_snapshot.py --date 2024-12-31
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
    read_table,
    overwrite_date_partition,
    write_batch_alerts,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("DailyStockSnapshot")


def run(snapshot_date: str) -> None:
    run_id = f"daily-snapshot-{snapshot_date}-{uuid.uuid4().hex[:8]}"
    logger.info("=" * 60)
    logger.info("DailyStockSnapshot START  date=%s  run_id=%s", snapshot_date, run_id)
    logger.info("=" * 60)

    spark = get_batch_session("DailyStockSnapshot")

    # ── 1. Load stock movements for the target date ───────────────────────────
    movements_sql = f"""
        SELECT product_id,
               store_id,
               movement_type,
               quantity,
               movement_date::date AS movement_date
        FROM   stock_movements
        WHERE  movement_date::date = '{snapshot_date}'
          AND  store_id IS NOT NULL
          AND  quantity != 0
    """
    movements = read_query(spark, movements_sql, "movements")
    movement_count = movements.count()
    logger.info("Loaded %d movement rows for %s", movement_count, snapshot_date)

    if movement_count == 0:
        logger.warning("No movements found for %s — writing zero-movement snapshot.", snapshot_date)

    # ── 2. Classify and aggregate movements per (product_id, store_id) ────────
    classified = (
        movements
        .withColumn(
            "inbound_qty",
            F.when(
                F.col("movement_type").isin("INBOUND", "RETURN") |
                ((F.col("movement_type") == "TRANSFER") & (F.col("quantity") > 0)),
                F.col("quantity")
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "outbound_qty",
            F.when(
                (F.col("movement_type") == "OUTBOUND") |
                ((F.col("movement_type") == "TRANSFER") & (F.col("quantity") < 0)),
                F.abs(F.col("quantity"))
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "adjustment_qty",
            F.when(F.col("movement_type") == "ADJUSTMENT", F.col("quantity"))
             .otherwise(F.lit(0))
        )
    )

    daily_totals = (
        classified
        .groupBy("product_id", "store_id")
        .agg(
            F.sum("inbound_qty").alias("total_inbound"),
            F.sum("outbound_qty").alias("total_outbound"),
            F.sum("adjustment_qty").alias("total_adjustment"),
        )
    )

    # ── 3. Load previous day's closing_stock as opening_stock ─────────────────
    prev_date = (date.fromisoformat(snapshot_date) - timedelta(days=1)).isoformat()
    prev_sql = f"""
        SELECT product_id,
               store_id,
               closing_stock AS prev_closing_stock
        FROM   batch_daily_stock_snapshot
        WHERE  snapshot_date = '{prev_date}'
    """
    try:
        prev_snapshot = read_query(spark, prev_sql, "prev_snapshot")
        has_prev = prev_snapshot.count() > 0
    except Exception:
        has_prev = False

    # ── 4. Load reorder_point from products ───────────────────────────────────
    products_sql = """
        SELECT product_id,
               reorder_point
        FROM   products
        WHERE  is_active = TRUE
    """
    products = read_query(spark, products_sql, "products")

    # ── 5. Join everything together ───────────────────────────────────────────
    if has_prev:
        with_opening = daily_totals.join(
            prev_snapshot, on=["product_id", "store_id"], how="left"
        ).withColumn(
            "opening_stock",
            F.coalesce(F.col("prev_closing_stock"), F.lit(0))
        ).drop("prev_closing_stock")
    else:
        # First run — seed opening stock from inventory_demand_stockout_risk
        seed_sql = f"""
            SELECT DISTINCT ON (product_id, store_id)
                   product_id,
                   store_id,
                   current_stock_units AS opening_stock
            FROM   inventory_demand_stockout_risk
            ORDER  BY product_id, store_id, snapshot_date DESC
        """
        try:
            seed = read_query(spark, seed_sql, "seed")
            with_opening = daily_totals.join(
                seed, on=["product_id", "store_id"], how="left"
            ).withColumn(
                "opening_stock",
                F.coalesce(F.col("opening_stock"), F.lit(0))
            )
        except Exception:
            with_opening = daily_totals.withColumn("opening_stock", F.lit(0))

    snapshot = (
        with_opening
        .join(products, on="product_id", how="left")
        .withColumn(
            "closing_stock",
            F.greatest(
                F.col("opening_stock")
                + F.col("total_inbound")
                - F.col("total_outbound")
                + F.col("total_adjustment"),
                F.lit(0)
            )
        )
        .withColumn("reorder_point", F.coalesce(F.col("reorder_point"), F.lit(50)))
        .withColumn(
            "is_below_reorder",
            F.col("closing_stock") < F.col("reorder_point")
        )
        .withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
        .select(
            "product_id", "store_id", "snapshot_date",
            "opening_stock", "total_inbound", "total_outbound",
            "total_adjustment", "closing_stock", "reorder_point",
            "is_below_reorder",
        )
    )

    row_count = snapshot.count()
    logger.info("Computed %d snapshot rows for %s", row_count, snapshot_date)

    # ── 6. Write snapshot (idempotent) ────────────────────────────────────────
    written = overwrite_date_partition(
        df=snapshot,
        table="batch_daily_stock_snapshot",
        date_col="snapshot_date",
        partition_date=snapshot_date,
        run_id=run_id,
    )
    logger.info("Wrote %d rows to batch_daily_stock_snapshot.", written)

    # ── 7. Write LOW_STOCK batch alerts for breached products ─────────────────
    below_reorder = snapshot.filter(F.col("is_below_reorder") == True)
    if not below_reorder.rdd.isEmpty():
        alerts = (
            below_reorder
            .withColumn(
                "alert_type",
                F.when(F.col("closing_stock") == 0, F.lit("STOCKOUT_RISK"))
                 .otherwise(F.lit("LOW_STOCK"))
            )
            .withColumn(
                "severity",
                F.when(F.col("closing_stock") == 0, F.lit("CRITICAL"))
                 .otherwise(F.lit("WARNING"))
            )
            .withColumn(
                "message",
                F.concat(
                    F.lit("[BATCH] "),
                    F.when(F.col("closing_stock") == 0, F.lit("STOCKOUT"))
                     .otherwise(F.lit("LOW STOCK")),
                    F.lit(": Product "), F.col("product_id"),
                    F.lit(" at store "), F.col("store_id"),
                    F.lit(" closed with "), F.col("closing_stock").cast("string"),
                    F.lit(" units on "), F.col("snapshot_date").cast("string"),
                    F.lit(" (reorder point: "), F.col("reorder_point").cast("string"),
                    F.lit(")")
                )
            )
            .withColumn("supplier_id", F.lit(None).cast("string"))
            .withColumn("metric_value",    F.col("closing_stock").cast(DecimalType(12, 2)))
            .withColumn("threshold_value", F.col("reorder_point").cast(DecimalType(12, 2)))
            .select(
                "alert_type", "severity", "product_id", "store_id",
                "supplier_id", "message", "metric_value", "threshold_value"
            )
        )
        alert_count = write_batch_alerts(alerts, run_id)
        logger.info("Raised %d LOW_STOCK/STOCKOUT batch alerts.", alert_count)

    logger.info("DailyStockSnapshot COMPLETE  run_id=%s", run_id)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default=(date.today() - timedelta(days=1)).isoformat(),
        help="Snapshot date in YYYY-MM-DD format (default: yesterday)",
    )
    args = parser.parse_args()
    run(args.date)
