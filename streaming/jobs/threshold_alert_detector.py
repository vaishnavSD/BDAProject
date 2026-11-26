"""
threshold_alert_detector.py
─────────────────────────────────────────────────────────────────────────────
Spark Structured Streaming Job 2 of 3

What it does:
  Consumes from inventory-live-orders and inventory-live-dispatches.
  For each micro-batch:
    1. Loads current stock snapshot from PostgreSQL (via JDBC static read).
    2. Computes net outbound quantity per (product_id, store_id).
    3. Joins with the stock snapshot to get the new estimated stock level.
    4. Joins with products table to get the reorder_point for each product.
    5. Where estimated_stock < reorder_point → writes a LOW_STOCK or
       STOCKOUT_RISK alert to the alerts table.
    6. Where estimated_stock == 0 → CRITICAL STOCKOUT_RISK alert.
    7. Where estimated_stock > reorder_point * 5 → OVERSTOCK alert.

Design note — why a static JDBC join inside foreachBatch:
  The authoritative stock level lives in PostgreSQL (written by the batch
  daily_stock_snapshot job and updated by INBOUND movements).
  Streaming cannot maintain global per-product state safely across restarts
  without an external state store. For this project scale, a JDBC read of
  ~1500 rows per micro-batch (products × stores) is acceptable (<100ms).
  In production, replace with a Redis lookup or Delta Lake state store.

Trigger: every STREAMING_TRIGGER_SECONDS (default 10s).
Checkpoint: SparkConfig.CHECKPOINT_DIR/threshold_detector/

Spark-submit command:
    spark-submit \
      --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      --jars /opt/bitnami/spark/jars/postgresql-42.7.3.jar \
      /opt/spark-apps/streaming/jobs/threshold_alert_detector.py
"""

import os
import sys
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

from config.settings import SparkConfig, DBConfig
from streaming.utils.spark_session import get_streaming_session
from streaming.utils.kafka_reader import read_live_orders, read_live_dispatches
from streaming.utils.db_writer import write_alerts_to_postgres

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("ThresholdAlertDetector")

CHECKPOINT = os.path.join(SparkConfig.CHECKPOINT_DIR, "threshold_detector")


# ─────────────────────────────────────────────────────────────────────────────
# Static reference loads (cached per micro-batch)
# ─────────────────────────────────────────────────────────────────────────────

def load_latest_stock_snapshot(spark: SparkSession) -> DataFrame:
    """
    Reads the most recent stock snapshot per (product_id, store_id) from
    batch_daily_stock_snapshot.  Returns closing_stock and reorder_point.

    If no batch snapshot exists yet (first run before any batch job has run),
    falls back to inventory_demand_stockout_risk which is pre-loaded from CSV.
    """
    jdbc_url   = DBConfig.jdbc_url()
    jdbc_props = DBConfig.jdbc_properties()

    # Try the batch snapshot table first
    snapshot_query = """
        (
          SELECT DISTINCT ON (product_id, store_id)
                 product_id,
                 store_id,
                 closing_stock      AS current_stock,
                 reorder_point
          FROM   batch_daily_stock_snapshot
          ORDER  BY product_id, store_id, snapshot_date DESC
        ) AS latest_snapshot
    """
    try:
        df = spark.read.jdbc(url=jdbc_url, table=snapshot_query, properties=jdbc_props)
        if df.count() > 0:
            return df
    except Exception:
        pass

    # Fallback: inventory_demand_stockout_risk (pre-loaded historical data)
    fallback_query = """
        (
          SELECT DISTINCT ON (product_id, store_id)
                 product_id,
                 store_id,
                 current_stock_units  AS current_stock,
                 reorder_point_units  AS reorder_point
          FROM   inventory_demand_stockout_risk
          ORDER  BY product_id, store_id, snapshot_date DESC
        ) AS risk_snapshot
    """
    df = spark.read.jdbc(url=jdbc_url, table=fallback_query, properties=jdbc_props)
    logger.info("Stock snapshot loaded from inventory_demand_stockout_risk (fallback).")
    return df


def load_product_reorder_points(spark: SparkSession) -> DataFrame:
    """
    Returns product_id → reorder_point from the products table.
    Used when the snapshot tables have no entry for a product yet.
    """
    query = "(SELECT product_id, reorder_point, unit_cost FROM products) AS prod_ref"
    return spark.read.jdbc(
        url=DBConfig.jdbc_url(),
        table=query,
        properties=DBConfig.jdbc_properties(),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Alert builders
# ─────────────────────────────────────────────────────────────────────────────

def build_low_stock_alerts(enriched_df: DataFrame) -> DataFrame:
    """
    Returns alert rows for products below reorder_point.

    Severity rules:
      estimated_stock == 0                        → CRITICAL  (STOCKOUT_RISK)
      0 < estimated_stock < reorder_point         → WARNING   (LOW_STOCK)
      estimated_stock >= reorder_point * 5        → INFO      (OVERSTOCK)
    """
    alerts = (
        enriched_df
        .filter(
            (F.col("estimated_stock") <= F.col("reorder_point")) |
            (F.col("estimated_stock") >= F.col("reorder_point") * 5)
        )
        .withColumn(
            "alert_type",
            F.when(F.col("estimated_stock") == 0,                      F.lit("STOCKOUT_RISK"))
             .when(F.col("estimated_stock") < F.col("reorder_point"),   F.lit("LOW_STOCK"))
             .otherwise(                                                 F.lit("OVERSTOCK"))
        )
        .withColumn(
            "severity",
            F.when(F.col("estimated_stock") == 0,                      F.lit("CRITICAL"))
             .when(F.col("estimated_stock") < F.col("reorder_point"),   F.lit("WARNING"))
             .otherwise(                                                 F.lit("INFO"))
        )
        .withColumn(
            "message",
            F.when(
                F.col("estimated_stock") == 0,
                F.concat(
                    F.lit("STOCKOUT: Product "), F.col("product_id"),
                    F.lit(" at store "), F.col("store_id"),
                    F.lit(" has 0 units remaining.")
                )
            ).when(
                F.col("estimated_stock") < F.col("reorder_point"),
                F.concat(
                    F.lit("LOW STOCK: Product "), F.col("product_id"),
                    F.lit(" at store "), F.col("store_id"),
                    F.lit(" has "), F.col("estimated_stock").cast("string"),
                    F.lit(" units (reorder point: "), F.col("reorder_point").cast("string"),
                    F.lit(" units).")
                )
            ).otherwise(
                F.concat(
                    F.lit("OVERSTOCK: Product "), F.col("product_id"),
                    F.lit(" at store "), F.col("store_id"),
                    F.lit(" has "), F.col("estimated_stock").cast("string"),
                    F.lit(" units (5× reorder point: "),
                    (F.col("reorder_point") * 5).cast("string"),
                    F.lit(" units).")
                )
            )
        )
        .select(
            F.col("alert_type"),
            F.col("severity"),
            F.lit("STREAMING").alias("source"),
            F.col("product_id"),
            F.col("store_id"),
            F.lit(None).cast("string").alias("supplier_id"),
            F.col("message"),
            F.col("estimated_stock").cast(DecimalType(12, 2)).alias("metric_value"),
            F.col("reorder_point").cast(DecimalType(12, 2)).alias("threshold_value"),
        )
    )
    return alerts


# ─────────────────────────────────────────────────────────────────────────────
# Micro-batch processor
# ─────────────────────────────────────────────────────────────────────────────

def detect_thresholds(batch_df: DataFrame, batch_id: int, spark: SparkSession) -> None:
    """
    foreachBatch handler: processes one micro-batch from the unified
    orders + dispatches stream and raises alerts where thresholds are breached.
    """
    if batch_df.rdd.isEmpty():
        logger.info("Batch %d: empty — skipping threshold detection.", batch_id)
        return

    logger.info("Batch %d: processing %d rows for threshold detection.", batch_id, batch_df.count())

    # ── 1. Compute net outbound per product per store in this batch ───────────
    # Orders:    outbound (demand)
    # Dispatches from_store: outbound
    # Dispatches to_store:   inbound (but tracked separately; simplified here)

    outbound = (
        batch_df
        .groupBy("product_id", "store_id")
        .agg(F.sum("outbound_qty").alias("batch_outbound"))
    )

    # ── 2. Load current stock snapshot ───────────────────────────────────────
    snapshot = load_latest_stock_snapshot(spark)

    # ── 3. Join: estimated_stock = current_stock − batch_outbound ────────────
    enriched = (
        snapshot
        .join(outbound, on=["product_id", "store_id"], how="left")
        .withColumn(
            "estimated_stock",
            F.greatest(
                F.col("current_stock") - F.coalesce(F.col("batch_outbound"), F.lit(0)),
                F.lit(0)   # stock cannot go below 0
            ).cast(IntegerType())
        )
        # Only evaluate products that had activity in this batch
        .filter(F.col("batch_outbound").isNotNull())
    )

    # ── 4. Build and write alerts ─────────────────────────────────────────────
    alerts = build_low_stock_alerts(enriched)

    if not alerts.rdd.isEmpty():
        alert_count = alerts.count()
        logger.info("Batch %d: raising %d alerts.", batch_id, alert_count)
        write_alerts_to_postgres(alerts, batch_id)
    else:
        logger.info("Batch %d: no threshold breaches detected.", batch_id)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    spark = get_streaming_session("ThresholdDetector")
    logger.info("ThresholdAlertDetector started. Checkpoint: %s", CHECKPOINT)

    # Read orders stream — each order is an outbound movement
    orders_stream = (
        read_live_orders(spark)
        .select(
            F.col("product_id"),
            F.col("store_id"),
            F.col("quantity_ordered").alias("outbound_qty"),
            F.col("event_ts"),
        )
    )

    # Read dispatches stream — from_store loses stock (outbound)
    dispatches_stream = (
        read_live_dispatches(spark)
        .select(
            F.col("product_id"),
            F.col("from_store_id").alias("store_id"),
            F.col("quantity_dispatched").alias("outbound_qty"),
            F.col("event_ts"),
        )
    )

    # Union both outbound sources into one stream
    unified_outbound = orders_stream.unionByName(dispatches_stream)

    # Create a closure that captures spark session
    def batch_handler(batch_df, batch_id):
        detect_thresholds(batch_df, batch_id, spark)

    query = (
        unified_outbound
        .writeStream
        .foreachBatch(batch_handler)
        .option("checkpointLocation", os.path.join(CHECKPOINT, "alerts"))
        .trigger(processingTime=f"{SparkConfig.STREAMING_TRIGGER_SECONDS} seconds")
        .outputMode("append")
        .queryName("threshold_alert_detector")
        .start()
    )

    logger.info("ThresholdAlertDetector running. Query ID: %s", query.id)
    query.awaitTermination()


if __name__ == "__main__":
    main()
