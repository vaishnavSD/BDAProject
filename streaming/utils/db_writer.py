"""
db_writer.py
─────────────────────────────────────────────────────────────────────────────
Shared JDBC write helpers for Spark Structured Streaming foreachBatch sinks.

Why foreachBatch and not a direct JDBC streaming sink?
  - The native JDBC streaming sink only supports append mode.
  - foreachBatch gives us full control: upsert logic, dedup within a batch,
    writing to multiple tables from one micro-batch, error handling.

Pattern used by every streaming job:
    def process_batch(batch_df, batch_id):
        write_to_postgres(batch_df, "table_name", batch_id, mode="append")

All writes go through write_to_postgres() which:
  1. Deduplicates the micro-batch on the PK before writing.
  2. Adds processed_at and spark_batch_id metadata columns.
  3. Uses INSERT ... ON CONFLICT DO NOTHING via mode="append" + unique PK.
     (PostgreSQL silently skips duplicates when the unique constraint fires.)
"""

import os
import sys
import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import DBConfig

logger = logging.getLogger(__name__)


def write_to_postgres(
    df: DataFrame,
    table_name: str,
    batch_id: int,
    pk_column: str,
    mode: str = "append",
) -> None:
    """
    Write a micro-batch DataFrame to a PostgreSQL table via JDBC.

    Parameters
    ----------
    df          : micro-batch DataFrame (already filtered / transformed)
    table_name  : target PostgreSQL table (schema-qualified if needed)
    batch_id    : Spark batch ID injected by foreachBatch
    pk_column   : primary key column used for intra-batch deduplication
    mode        : 'append' (default) or 'overwrite'
    """
    if df is None or df.rdd.isEmpty():
        logger.info("Batch %d: empty micro-batch for table %s — skipping.", batch_id, table_name)
        return

    run_id = f"stream-batch-{batch_id}"
    now_ts = datetime.now(timezone.utc).isoformat()

    # ── Add Spark processing metadata columns ─────────────────────────────────
    df_with_meta = (
        df
        .withColumn("processed_at",    F.lit(now_ts).cast("timestamp"))
        .withColumn("spark_batch_id",  F.lit(run_id))
        .withColumn("kafka_topic",     F.lit(table_name))   # topic name as label
    )

    # ── Intra-batch deduplication on PK ──────────────────────────────────────
    # If the same event was published twice within the same micro-batch,
    # keep only the last occurrence (highest kafka_offset).
    if "kafka_offset" in df_with_meta.columns:
        from pyspark.sql.window import Window
        window = Window.partitionBy(pk_column).orderBy(F.col("kafka_offset").desc())
        df_with_meta = (
            df_with_meta
            .withColumn("_row_num", F.row_number().over(window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num", "kafka_offset", "kafka_partition")
        )
    else:
        df_with_meta = df_with_meta.dropDuplicates([pk_column])

    row_count = df_with_meta.count()
    logger.info("Batch %d: writing %d rows to %s", batch_id, row_count, table_name)

    # ── JDBC Write ────────────────────────────────────────────────────────────
    (
        df_with_meta
        .write
        .format("jdbc")
        .option("url",      DBConfig.jdbc_url())
        .option("dbtable",  table_name)
        .option("user",     DBConfig.USER)
        .option("password", DBConfig.PASSWORD)
        .option("driver",   "org.postgresql.Driver")
        # Connection pool settings
        .option("numPartitions",           "2")
        .option("batchsize",               "1000")
        .option("isolationLevel",          "READ_COMMITTED")
        .mode(mode)
        .save()
    )

    logger.info("Batch %d: committed %d rows to %s.", batch_id, row_count, table_name)


def write_alerts_to_postgres(
    alerts_df: DataFrame,
    batch_id: int,
) -> None:
    """
    Specialised writer for the alerts table.
    alerts_df must already have all alerts columns except alert_id (BIGSERIAL).
    Uses append mode; PostgreSQL auto-assigns alert_id.
    """
    if alerts_df is None or alerts_df.rdd.isEmpty():
        return

    run_id = f"stream-batch-{batch_id}"

    alerts_with_meta = (
        alerts_df
        .withColumn("spark_job_run_id", F.lit(run_id))
        .withColumn("created_at",       F.current_timestamp())
        .withColumn("is_resolved",      F.lit(False))
    )

    (
        alerts_with_meta
        .write
        .format("jdbc")
        .option("url",      DBConfig.jdbc_url())
        .option("dbtable",  "alerts")
        .option("user",     DBConfig.USER)
        .option("password", DBConfig.PASSWORD)
        .option("driver",   "org.postgresql.Driver")
        .option("numPartitions", "1")
        .option("batchsize",     "500")
        .mode("append")
        .save()
    )

    count = alerts_with_meta.count()
    logger.info("Batch %d: wrote %d alerts to alerts table.", batch_id, count)
