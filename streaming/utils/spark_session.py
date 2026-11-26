"""
spark_session.py
─────────────────────────────────────────────────────────────────────────────
Single factory for creating SparkSessions used by all streaming jobs.

Design decisions:
  - Kafka connector JAR loaded via --packages at spark-submit time.
    Here we only set it as a fallback for local testing.
  - PostgreSQL JDBC driver is pre-installed in the Docker image
    at /opt/bitnami/spark/jars/postgresql-42.7.3.jar.
  - All Spark config lives here; individual jobs never call SparkSession
    directly — they call get_streaming_session() or get_batch_session().
  - Adaptive Query Execution (AQE) is enabled for dynamic partition coalescing.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import SparkSession
from config.settings import SparkConfig, DBConfig


def get_streaming_session(app_suffix: str = "") -> SparkSession:
    """
    Returns a SparkSession configured for Structured Streaming.
    app_suffix  — appended to the app name so each job is identifiable
                  in the Spark Web UI (e.g. 'StockAggregator').
    """
    app_name = f"{SparkConfig.APP_NAME}-{app_suffix}" if app_suffix else SparkConfig.APP_NAME

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(SparkConfig.MASTER_URL)

        # ── Kafka connector (fallback for local dev; use --packages in prod) ──
        .config(
            "spark.jars.packages",
            SparkConfig.KAFKA_PACKAGES,
        )

        # ── Serialiser: Kryo is faster than Java default serialiser ──────────
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # ── Adaptive Query Execution ─────────────────────────────────────────
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # ── Streaming ────────────────────────────────────────────────────────
        # Stop gracefully on JVM shutdown (SIGTERM from Docker)
        .config("spark.streaming.stopGracefullyOnShutdown", "true")

        # ── Checkpoint location (shared across jobs via subdirectory) ────────
        # Individual jobs append their own subdir; e.g. /opt/spark-checkpoints/stock_agg
        .config("spark.sql.streaming.checkpointLocation", SparkConfig.CHECKPOINT_DIR)

        # ── Shuffle partitions: 200 is too high for small cluster ────────────
        .config("spark.sql.shuffle.partitions", "8")

        # ── Timezone: all timestamps stored and compared in UTC ──────────────
        .config("spark.sql.session.timeZone", "UTC")

        .getOrCreate()
    )

    # Reduce Spark's own verbose INFO logging to WARNING in console
    spark.sparkContext.setLogLevel("WARN")

    return spark


def get_batch_session(app_suffix: str = "") -> SparkSession:
    """
    Returns a SparkSession for batch jobs.
    Same config as streaming but without streaming-specific settings.
    """
    app_name = f"{SparkConfig.APP_NAME}-Batch-{app_suffix}" if app_suffix else f"{SparkConfig.APP_NAME}-Batch"

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(SparkConfig.MASTER_URL)
        .config("spark.jars.packages", SparkConfig.KAFKA_PACKAGES)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
