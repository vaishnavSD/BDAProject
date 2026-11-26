"""
batch/utils/jdbc_helpers.py
─────────────────────────────────────────────────────────────────────────────
Shared JDBC read/write helpers for all four batch jobs.

Read patterns:
  read_table()       — full table load
  read_query()       — arbitrary SQL pushed down to PostgreSQL
  read_date_range()  — time-bounded read (used by incremental jobs)

Write patterns:
  upsert_to_postgres()   — write with ON CONFLICT DO UPDATE via temp table trick
  append_to_postgres()   — simple append (for insert-only report tables)
  overwrite_partition()  — overwrite a date partition (idempotent daily runs)
"""

import os
import sys
import logging
from datetime import date
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import DBConfig

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# READ HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def read_table(spark: SparkSession, table: str) -> DataFrame:
    """Full table read via JDBC."""
    logger.info("Reading table: %s", table)
    return (
        spark.read
        .format("jdbc")
        .option("url",      DBConfig.jdbc_url())
        .option("dbtable",  table)
        .option("user",     DBConfig.USER)
        .option("password", DBConfig.PASSWORD)
        .option("driver",   "org.postgresql.Driver")
        .option("fetchsize", "5000")
        .load()
    )


def read_query(spark: SparkSession, sql: str, alias: str = "subquery") -> DataFrame:
    """
    Push an arbitrary SQL query down to PostgreSQL.
    sql must be a complete SELECT statement.
    alias is the table alias used in the JDBC dbtable option.
    """
    wrapped = f"({sql}) AS {alias}"
    logger.info("Executing pushdown query: %s...", sql[:80])
    return (
        spark.read
        .format("jdbc")
        .option("url",      DBConfig.jdbc_url())
        .option("dbtable",  wrapped)
        .option("user",     DBConfig.USER)
        .option("password", DBConfig.PASSWORD)
        .option("driver",   "org.postgresql.Driver")
        .option("fetchsize", "5000")
        .load()
    )


def read_date_range(
    spark: SparkSession,
    table: str,
    date_col: str,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """
    Read rows from table where date_col is between start_date and end_date.
    Uses predicate pushdown — PostgreSQL filters before data is transferred.
    """
    sql = f"""
        SELECT *
        FROM   {table}
        WHERE  {date_col} >= '{start_date}'
          AND  {date_col} <= '{end_date}'
    """
    return read_query(spark, sql, alias=f"{table}_range")


# ─────────────────────────────────────────────────────────────────────────────
# WRITE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def append_to_postgres(
    df: DataFrame,
    table: str,
    run_id: str,
    batch_size: int = 1000,
) -> int:
    """
    Append rows to a PostgreSQL table.
    Adds spark_job_run_id and computed_at before writing.
    Returns the row count written.
    """
    df_with_meta = (
        df
        .withColumn("spark_job_run_id", F.lit(run_id))
        .withColumn("computed_at",      F.current_timestamp())
    )

    row_count = df_with_meta.count()
    logger.info("Appending %d rows to %s (run_id=%s)", row_count, table, run_id)

    (
        df_with_meta
        .write
        .format("jdbc")
        .option("url",           DBConfig.jdbc_url())
        .option("dbtable",       table)
        .option("user",          DBConfig.USER)
        .option("password",      DBConfig.PASSWORD)
        .option("driver",        "org.postgresql.Driver")
        .option("batchsize",     str(batch_size))
        .option("numPartitions", "4")
        .option("isolationLevel","READ_COMMITTED")
        .mode("append")
        .save()
    )

    logger.info("Wrote %d rows to %s.", row_count, table)
    return row_count


def overwrite_date_partition(
    df: DataFrame,
    table: str,
    date_col: str,
    partition_date: str,
    run_id: str,
) -> int:
    """
    Idempotent write for daily report tables.

    Strategy:
      1. Delete existing rows for partition_date from the table via JDBC.
      2. Append the new computed rows.
    This avoids full table overwrites while keeping daily runs re-runnable.

    Note: Step 1 uses psycopg2 directly (Spark has no DELETE support).
    """
    import psycopg2

    # Step 1: delete existing partition
    logger.info("Deleting existing rows for %s = %s in %s", date_col, partition_date, table)
    conn = psycopg2.connect(
        host=DBConfig.HOST,
        port=DBConfig.PORT,
        dbname=DBConfig.DB,
        user=DBConfig.USER,
        password=DBConfig.PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {table} WHERE {date_col} = %s",
                (partition_date,)
            )
            deleted = cur.rowcount
        conn.commit()
        logger.info("Deleted %d existing rows from %s for date %s.", deleted, table, partition_date)
    finally:
        conn.close()

    # Step 2: append fresh rows
    return append_to_postgres(df, table, run_id)


def write_batch_alerts(
    df: DataFrame,
    run_id: str,
) -> int:
    """
    Append batch-generated alerts to the alerts table.
    source is always 'BATCH'.
    """
    df_alerts = (
        df
        .withColumn("source",           F.lit("BATCH"))
        .withColumn("is_resolved",      F.lit(False))
        .withColumn("spark_job_run_id", F.lit(run_id))
        .withColumn("created_at",       F.current_timestamp())
    )

    row_count = df_alerts.count()
    if row_count == 0:
        logger.info("No batch alerts to write.")
        return 0

    logger.info("Writing %d batch alerts (run_id=%s)", row_count, run_id)

    (
        df_alerts
        .write
        .format("jdbc")
        .option("url",           DBConfig.jdbc_url())
        .option("dbtable",       "alerts")
        .option("user",          DBConfig.USER)
        .option("password",      DBConfig.PASSWORD)
        .option("driver",        "org.postgresql.Driver")
        .option("batchsize",     "500")
        .option("numPartitions", "1")
        .mode("append")
        .save()
    )

    return row_count
