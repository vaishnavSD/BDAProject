"""
batch/utils/spark_session.py
─────────────────────────────────────────────────────────────────────────────
Spark session factory for batch jobs.
Reuses the same factory from streaming/utils but adds batch-specific tuning:
  - Higher shuffle partitions (better parallelism on larger datasets)
  - Speculative execution off (batch jobs have predictable task times)
  - Dynamic allocation off (fixed resources per Airflow-triggered job)
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import SparkSession
from config.settings import SparkConfig


def get_batch_session(app_name: str) -> SparkSession:
    """
    Returns a fully configured SparkSession for a batch job.
    app_name should be short and descriptive, e.g. 'DailyStockSnapshot'.
    """
    spark = (
        SparkSession.builder
        .appName(f"{SparkConfig.APP_NAME}-{app_name}")
        .master(SparkConfig.MASTER_URL)
        .config("spark.jars.packages",                    SparkConfig.KAFKA_PACKAGES)
        .config("spark.serializer",                       "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled",             "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions",           "16")
        .config("spark.sql.session.timeZone",             "UTC")
        # Batch jobs write large result sets; use more JDBC partitions
        .config("spark.jdbc.numPartitions",               "4")
        # Avoid speculative re-execution in batch — deterministic jobs
        .config("spark.speculation",                      "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
