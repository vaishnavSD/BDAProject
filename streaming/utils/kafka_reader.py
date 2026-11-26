"""
kafka_reader.py
─────────────────────────────────────────────────────────────────────────────
Shared utilities for reading from Kafka topics in Spark Structured Streaming.

All three streaming jobs use the same read pattern:
  1. Read raw bytes from Kafka.
  2. Parse the JSON value into typed columns using an explicit schema.
  3. Add a watermark on event_timestamp for late-data handling.

Explicit schemas are defined here — never use schema inference on streaming
sources; it triggers an extra Kafka read and fails on empty topics.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType, DecimalType, LongType
)
from config.settings import KafkaConfig, SparkConfig


# ─────────────────────────────────────────────────────────────────────────────
# JSON Schemas — must match exactly what producers publish via to_kafka_dict()
# ─────────────────────────────────────────────────────────────────────────────

LIVE_ORDER_SCHEMA = StructType([
    StructField("live_order_id",    StringType(),    False),
    StructField("product_id",       StringType(),    False),
    StructField("store_id",         StringType(),    False),
    StructField("supplier_id",      StringType(),    False),
    StructField("event_timestamp",  StringType(),    False),   # parsed to timestamp below
    StructField("quantity_ordered", IntegerType(),   False),
    StructField("unit_price",       StringType(),    False),   # Decimal serialised as string
    StructField("total_amount",     StringType(),    False),
    StructField("order_status",     StringType(),    False),
    StructField("event_type",       StringType(),    False),
    StructField("source_system",    StringType(),    False),
])

LIVE_SHIPMENT_SCHEMA = StructType([
    StructField("live_shipment_id",    StringType(),  False),
    StructField("shipment_id",         StringType(),  True),
    StructField("product_id",          StringType(),  False),
    StructField("store_id",            StringType(),  False),
    StructField("event_timestamp",     StringType(),  False),
    StructField("shipment_status",     StringType(),  False),
    StructField("current_location",    StringType(),  True),
    StructField("estimated_arrival",   StringType(),  True),
    StructField("quantity_in_transit", IntegerType(), False),
    StructField("event_type",          StringType(),  False),
    StructField("carrier",             StringType(),  True),
])

LIVE_DISPATCH_SCHEMA = StructType([
    StructField("dispatch_id",          StringType(),  False),
    StructField("product_id",           StringType(),  False),
    StructField("from_store_id",        StringType(),  False),
    StructField("to_store_id",          StringType(),  False),
    StructField("event_timestamp",      StringType(),  False),
    StructField("quantity_dispatched",  IntegerType(), False),
    StructField("dispatch_reason",      StringType(),  False),
    StructField("dispatch_status",      StringType(),  False),
    StructField("event_type",           StringType(),  False),
    StructField("priority",             StringType(),  False),
])


# ─────────────────────────────────────────────────────────────────────────────
# Reader factory
# ─────────────────────────────────────────────────────────────────────────────

def _read_kafka_raw(spark: SparkSession, topic: str) -> DataFrame:
    """
    Returns a raw streaming DataFrame from a single Kafka topic.
    Columns: key (binary), value (binary), topic, partition, offset,
             timestamp (Kafka broker timestamp), timestampType.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KafkaConfig.BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")       # production: latest
        .option("failOnDataLoss", "false")          # allow topic gaps on restart
        .option("maxOffsetsPerTrigger", 1000)       # back-pressure: cap per micro-batch
        .load()
    )


def read_live_orders(spark: SparkSession) -> DataFrame:
    """
    Returns parsed streaming DataFrame for the live-orders topic.
    Adds:
      - event_ts       : proper TimestampType from ISO string
      - kafka_offset   : Kafka offset for lineage tracking
      - kafka_partition: partition number
    Watermark: 5 minutes on event_ts.
    """
    raw = _read_kafka_raw(spark, KafkaConfig.TOPIC_LIVE_ORDERS)

    parsed = (
        raw
        .select(
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.from_json(
                F.col("value").cast("string"),
                LIVE_ORDER_SCHEMA
            ).alias("data")
        )
        .select(
            "kafka_offset",
            "kafka_partition",
            "data.*"
        )
        # Parse ISO timestamp string → TimestampType
        .withColumn("event_ts", F.to_timestamp(F.col("event_timestamp")))
        # Cast Decimal strings
        .withColumn("unit_price",   F.col("unit_price").cast(DecimalType(10, 2)))
        .withColumn("total_amount", F.col("total_amount").cast(DecimalType(14, 2)))
        # Drop the original string timestamp column
        .drop("event_timestamp")
        # Watermark for late data tolerance
        .withWatermark("event_ts", f"{SparkConfig.WATERMARK_MINUTES} minutes")
    )
    return parsed


def read_live_shipments(spark: SparkSession) -> DataFrame:
    """
    Returns parsed streaming DataFrame for the live-shipments topic.
    Watermark: 5 minutes on event_ts.
    """
    raw = _read_kafka_raw(spark, KafkaConfig.TOPIC_LIVE_SHIPMENTS)

    parsed = (
        raw
        .select(
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.from_json(
                F.col("value").cast("string"),
                LIVE_SHIPMENT_SCHEMA
            ).alias("data")
        )
        .select("kafka_offset", "kafka_partition", "data.*")
        .withColumn("event_ts", F.to_timestamp(F.col("event_timestamp")))
        .withColumn(
            "estimated_arrival_date",
            F.to_date(F.col("estimated_arrival"), "yyyy-MM-dd")
        )
        .drop("event_timestamp", "estimated_arrival")
        .withWatermark("event_ts", f"{SparkConfig.WATERMARK_MINUTES} minutes")
    )
    return parsed


def read_live_dispatches(spark: SparkSession) -> DataFrame:
    """
    Returns parsed streaming DataFrame for the live-dispatches topic.
    Watermark: 5 minutes on event_ts.
    """
    raw = _read_kafka_raw(spark, KafkaConfig.TOPIC_LIVE_DISPATCHES)

    parsed = (
        raw
        .select(
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.from_json(
                F.col("value").cast("string"),
                LIVE_DISPATCH_SCHEMA
            ).alias("data")
        )
        .select("kafka_offset", "kafka_partition", "data.*")
        .withColumn("event_ts", F.to_timestamp(F.col("event_timestamp")))
        .drop("event_timestamp")
        .withWatermark("event_ts", f"{SparkConfig.WATERMARK_MINUTES} minutes")
    )
    return parsed
