"""
shipment_delay_monitor.py
─────────────────────────────────────────────────────────────────────────────
Spark Structured Streaming Job 3 of 3

What it does:
  Consumes from inventory-live-shipments.
  For each micro-batch:
    1. Identifies shipments that are DELAYED or still IN_TRANSIT past their
       estimated_arrival date.
    2. Computes how many days overdue each shipment is.
    3. Looks up the supplier for the shipment's product via JDBC.
    4. Writes a SHIPMENT_DELAY or SUPPLIER_DELAY alert to the alerts table.
    5. Writes a REORDER_TRIGGERED alert if a delayed shipment causes estimated
       stock to fall below reorder_point (joins with stock snapshot).

Alert severity rules:
  delay_days <= 2   → WARNING   (minor delay, monitor)
  delay_days  3–7   → WARNING   (escalate to supplier)
  delay_days > 7    → CRITICAL  (source alternative supplier)

Trigger: every STREAMING_TRIGGER_SECONDS (default 10s).
Checkpoint: SparkConfig.CHECKPOINT_DIR/shipment_monitor/

Spark-submit command:
    spark-submit \
      --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      --jars /opt/bitnami/spark/jars/postgresql-42.7.3.jar \
      /opt/spark-apps/streaming/jobs/shipment_delay_monitor.py
"""

import os
import sys
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from config.settings import SparkConfig, DBConfig
from streaming.utils.spark_session import get_streaming_session
from streaming.utils.kafka_reader import read_live_shipments
from streaming.utils.db_writer import write_alerts_to_postgres

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("ShipmentDelayMonitor")

CHECKPOINT = os.path.join(SparkConfig.CHECKPOINT_DIR, "shipment_monitor")


# ─────────────────────────────────────────────────────────────────────────────
# Reference loaders
# ─────────────────────────────────────────────────────────────────────────────

def load_product_supplier_map(spark: SparkSession) -> DataFrame:
    """
    Returns product_id → supplier_id mapping from the products table.
    Used to populate supplier_id on shipment delay alerts.
    """
    query = "(SELECT product_id, supplier_id FROM products WHERE is_active = TRUE) AS prod_sup"
    return spark.read.jdbc(
        url=DBConfig.jdbc_url(),
        table=query,
        properties=DBConfig.jdbc_properties(),
    )


def load_open_shipments_reference(spark: SparkSession) -> DataFrame:
    """
    Returns IN_TRANSIT and DELAYED shipments from the batch shipments table.
    Used to cross-reference shipment_id from streaming events.
    """
    query = """
        (
          SELECT shipment_id,
                 supplier_id,
                 estimated_arrival,
                 store_id
          FROM   shipments
          WHERE  status IN ('IN_TRANSIT', 'DELAYED')
        ) AS open_shipments
    """
    return spark.read.jdbc(
        url=DBConfig.jdbc_url(),
        table=query,
        properties=DBConfig.jdbc_properties(),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Micro-batch processor
# ─────────────────────────────────────────────────────────────────────────────

def monitor_delays(batch_df: DataFrame, batch_id: int, spark: SparkSession) -> None:
    """
    foreachBatch handler: processes one micro-batch of shipment events.
    Raises alerts for delayed and overdue shipments.
    """
    if batch_df.rdd.isEmpty():
        logger.info("Batch %d: empty shipment batch — skipping.", batch_id)
        return

    total = batch_df.count()
    logger.info("Batch %d: processing %d shipment events.", batch_id, total)

    # ── 1. Filter to actionable statuses ─────────────────────────────────────
    actionable = batch_df.filter(
        F.col("shipment_status").isin("DELAYED", "IN_TRANSIT", "OUT_FOR_DELIVERY")
    )

    if actionable.rdd.isEmpty():
        logger.info("Batch %d: no delayed/in-transit shipments.", batch_id)
        return

    # ── 2. Compute delay days for events that have an estimated_arrival ───────
    today = F.current_date()

    with_delay = (
        actionable
        .withColumn(
            "delay_days",
            F.when(
                F.col("estimated_arrival_date").isNotNull(),
                F.datediff(today, F.col("estimated_arrival_date"))
            ).otherwise(F.lit(0))
        )
    )

    # Only alert if actually overdue (delay_days > 0) or status is DELAYED
    overdue = with_delay.filter(
        (F.col("delay_days") > 0) | (F.col("shipment_status") == "DELAYED")
    )

    if overdue.rdd.isEmpty():
        logger.info("Batch %d: no overdue shipments.", batch_id)
        return

    overdue_count = overdue.count()
    logger.info("Batch %d: %d overdue/delayed shipments found.", batch_id, overdue_count)

    # ── 3. Enrich with supplier_id from products table ────────────────────────
    prod_sup = load_product_supplier_map(spark)
    enriched = overdue.join(prod_sup, on="product_id", how="left")

    # ── 4. Build alerts ───────────────────────────────────────────────────────
    alerts = (
        enriched
        .withColumn(
            "severity",
            F.when(F.col("delay_days") > 7,  F.lit("CRITICAL"))
             .when(F.col("delay_days") > 2,  F.lit("WARNING"))
             .otherwise(                     F.lit("WARNING"))
        )
        .withColumn(
            "alert_type",
            F.when(
                F.col("supplier_id").isNotNull(),
                F.lit("SUPPLIER_DELAY")
            ).otherwise(
                F.lit("SHIPMENT_DELAY")
            )
        )
        .withColumn(
            "message",
            F.concat(
                F.lit("Shipment "), F.col("shipment_id"),
                F.lit(" ["), F.col("shipment_status"), F.lit("]"),
                F.lit(" carrying "), F.col("quantity_in_transit").cast("string"),
                F.lit(" units of "), F.col("product_id"),
                F.lit(" is "), F.col("delay_days").cast("string"),
                F.lit(" day(s) overdue. Location: "), F.col("current_location"),
            )
        )
        .select(
            F.col("alert_type"),
            F.col("severity"),
            F.lit("STREAMING").alias("source"),
            F.col("product_id"),
            F.col("store_id"),
            F.col("supplier_id"),
            F.col("message"),
            F.col("delay_days").cast(DecimalType(12, 2)).alias("metric_value"),
            F.lit(0).cast(DecimalType(12, 2)).alias("threshold_value"),
        )
    )

    # ── 5. Write alerts to PostgreSQL ─────────────────────────────────────────
    write_alerts_to_postgres(alerts, batch_id)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    spark = get_streaming_session("ShipmentMonitor")
    logger.info("ShipmentDelayMonitor started. Checkpoint: %s", CHECKPOINT)

    shipments_stream = read_live_shipments(spark)

    def batch_handler(batch_df, batch_id):
        monitor_delays(batch_df, batch_id, spark)

    query = (
        shipments_stream
        .writeStream
        .foreachBatch(batch_handler)
        .option("checkpointLocation", os.path.join(CHECKPOINT, "delays"))
        .trigger(processingTime=f"{SparkConfig.STREAMING_TRIGGER_SECONDS} seconds")
        .outputMode("append")
        .queryName("shipment_delay_monitor")
        .start()
    )

    logger.info("ShipmentDelayMonitor running. Query ID: %s", query.id)
    query.awaitTermination()


if __name__ == "__main__":
    main()
