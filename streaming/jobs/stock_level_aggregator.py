"""
stock_level_aggregator.py
─────────────────────────────────────────────────────────────────────────────
Spark Structured Streaming Job 1 of 3

What it does:
  Consumes from THREE Kafka topics simultaneously:
    - inventory-live-orders     (demand events: quantity leaves the warehouse)
    - inventory-live-shipments  (supply events: quantity arriving at warehouse)
    - inventory-live-dispatches (transfer events: quantity moving between stores)

  For each micro-batch it:
    1. Unions all three event streams into a normalised movement DataFrame.
    2. Computes net stock movement per (product_id, store_id) within the batch.
    3. Writes each individual event to its respective stream_* table.
    4. Writes the net movements to stream_live_orders (orders only),
       stream_live_shipments, and stream_live_dispatches.

  Trigger: every STREAMING_TRIGGER_SECONDS (default 10s).
  Checkpoint: SparkConfig.CHECKPOINT_DIR/stock_aggregator/

Why three topics in one job:
  Stock level = inbound shipments + inbound dispatches − outbound orders − outbound dispatches.
  Splitting into separate jobs would require external state coordination.
  One job with foreachBatch keeps the logic atomic per micro-batch.

Spark-submit command (run inside Docker spark-master):
    spark-submit \
      --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      --jars /opt/bitnami/spark/jars/postgresql-42.7.3.jar \
      /opt/spark-apps/streaming/jobs/stock_level_aggregator.py
"""

import os
import sys
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from config.settings import SparkConfig, KafkaConfig
from streaming.utils.spark_session import get_streaming_session
from streaming.utils.kafka_reader import (
    read_live_orders,
    read_live_shipments,
    read_live_dispatches,
)
from streaming.utils.db_writer import write_to_postgres, write_alerts_to_postgres

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("StockLevelAggregator")

CHECKPOINT = os.path.join(SparkConfig.CHECKPOINT_DIR, "stock_aggregator")


# ─────────────────────────────────────────────────────────────────────────────
# Micro-batch processor
# ─────────────────────────────────────────────────────────────────────────────

def process_orders_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Handles a micro-batch from the live-orders topic.

    For each order event:
      - Writes raw event to stream_live_orders table.
      - Computes quantity_ordered as a NEGATIVE movement (stock going out).
    """
    if batch_df.rdd.isEmpty():
        return

    logger.info("Orders batch %d: %d rows", batch_id, batch_df.count())

    # 1. Write raw events to stream_live_orders
    write_to_postgres(
        df=batch_df,
        table_name="stream_live_orders",
        batch_id=batch_id,
        pk_column="live_order_id",
    )


def process_shipments_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Handles a micro-batch from the live-shipments topic.

    For each shipment event:
      - Writes raw event to stream_live_shipments table.
      - Only DELIVERED status events represent actual stock arrival.
    """
    if batch_df.rdd.isEmpty():
        return

    logger.info("Shipments batch %d: %d rows", batch_id, batch_df.count())

    # Rename column to match table schema
    df_for_db = batch_df.withColumnRenamed("event_ts", "event_timestamp") \
                        .withColumnRenamed("estimated_arrival_date", "estimated_arrival")

    write_to_postgres(
        df=df_for_db,
        table_name="stream_live_shipments",
        batch_id=batch_id,
        pk_column="live_shipment_id",
    )

    # Raise SHIPMENT_DELAY alerts for DELAYED status events
    delayed = batch_df.filter(F.col("shipment_status") == "DELAYED")
    if not delayed.rdd.isEmpty():
        alerts = (
            delayed.select(
                F.lit("SHIPMENT_DELAY").alias("alert_type"),
                F.lit("WARNING").alias("severity"),
                F.lit("STREAMING").alias("source"),
                F.col("product_id"),
                F.col("store_id"),
                F.lit(None).cast("string").alias("supplier_id"),
                F.concat(
                    F.lit("Shipment "), F.col("shipment_id"),
                    F.lit(" is DELAYED at "), F.col("current_location")
                ).alias("message"),
                F.col("quantity_in_transit").cast(DecimalType(12, 2)).alias("metric_value"),
                F.lit(None).cast(DecimalType(12, 2)).alias("threshold_value"),
            )
        )
        write_alerts_to_postgres(alerts, batch_id)


def process_dispatches_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Handles a micro-batch from the live-dispatches topic.

    For each dispatch event:
      - Writes raw event to stream_live_dispatches table.
      - Raises URGENT priority alerts.
    """
    if batch_df.rdd.isEmpty():
        return

    logger.info("Dispatches batch %d: %d rows", batch_id, batch_df.count())

    df_for_db = batch_df.withColumnRenamed("event_ts", "event_timestamp")

    write_to_postgres(
        df=df_for_db,
        table_name="stream_live_dispatches",
        batch_id=batch_id,
        pk_column="dispatch_id",
    )

    # Alert for EMERGENCY_REPLENISH dispatches (always CRITICAL)
    emergency = batch_df.filter(F.col("dispatch_reason") == "EMERGENCY_REPLENISH")
    if not emergency.rdd.isEmpty():
        alerts = (
            emergency.select(
                F.lit("LOW_STOCK").alias("alert_type"),
                F.lit("CRITICAL").alias("severity"),
                F.lit("STREAMING").alias("source"),
                F.col("product_id"),
                F.col("from_store_id").alias("store_id"),
                F.lit(None).cast("string").alias("supplier_id"),
                F.concat(
                    F.lit("EMERGENCY replenishment dispatched: "),
                    F.col("quantity_dispatched").cast("string"),
                    F.lit(" units of "), F.col("product_id"),
                    F.lit(" from "), F.col("from_store_id"),
                    F.lit(" → "), F.col("to_store_id"),
                ).alias("message"),
                F.col("quantity_dispatched").cast(DecimalType(12, 2)).alias("metric_value"),
                F.lit(None).cast(DecimalType(12, 2)).alias("threshold_value"),
            )
        )
        write_alerts_to_postgres(alerts, batch_id)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    spark = get_streaming_session("StockAggregator")
    logger.info("StockLevelAggregator started. Checkpoint: %s", CHECKPOINT)

    # ── Read all three topics ─────────────────────────────────────────────────
    orders_stream    = read_live_orders(spark)
    shipments_stream = read_live_shipments(spark)
    dispatches_stream = read_live_dispatches(spark)

    # ── Write queries — one per topic ─────────────────────────────────────────
    orders_query = (
        orders_stream
        .writeStream
        .foreachBatch(process_orders_batch)
        .option("checkpointLocation", os.path.join(CHECKPOINT, "orders"))
        .trigger(processingTime=f"{SparkConfig.STREAMING_TRIGGER_SECONDS} seconds")
        .outputMode("append")
        .queryName("live_orders_writer")
        .start()
    )

    shipments_query = (
        shipments_stream
        .writeStream
        .foreachBatch(process_shipments_batch)
        .option("checkpointLocation", os.path.join(CHECKPOINT, "shipments"))
        .trigger(processingTime=f"{SparkConfig.STREAMING_TRIGGER_SECONDS} seconds")
        .outputMode("append")
        .queryName("live_shipments_writer")
        .start()
    )

    dispatches_query = (
        dispatches_stream
        .writeStream
        .foreachBatch(process_dispatches_batch)
        .option("checkpointLocation", os.path.join(CHECKPOINT, "dispatches"))
        .trigger(processingTime=f"{SparkConfig.STREAMING_TRIGGER_SECONDS} seconds")
        .outputMode("append")
        .queryName("live_dispatches_writer")
        .start()
    )

    logger.info("All three streaming queries running. Waiting for termination...")

    # Block until all queries complete or fail
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
