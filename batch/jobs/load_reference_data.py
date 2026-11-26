"""
load_reference_data.py
─────────────────────────────────────────────────────────────────────────────
Batch Job 0 — One-time / weekly refresh (triggered by Airflow init DAG)

Loads all 8 non-streaming CSVs into PostgreSQL using Spark, applying:
  1. Schema enforcement (typed columns, not all-string)
  2. Data quality filtering (duplicates, invalid enums, null required fields)
  3. Quarantine: rejected rows written to staging_quarantine via psycopg2
  4. Deduplication before JDBC write

Load order (respects FK dependencies):
  suppliers → stores → products →
  orders → shipments → stock_movements →
  historical_sales → inventory_demand_stockout_risk

Mode per table:
  Reference tables (suppliers/stores/products):  overwrite (full refresh)
  Transactional tables (orders/shipments/moves):  append with dedup check
  Historical tables (sales/risk):                 append with dedup check

Arguments:
  --mode  full | reference-only | historical-only
          full = all tables (default)
"""

import argparse
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import psycopg2
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType
)

from batch.utils.spark_session import get_batch_session
from config.settings import DBConfig, DataPaths

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("LoadReferenceData")

RUN_ID = f"ref-load-{uuid.uuid4().hex[:8]}"

# ─────────────────────────────────────────────────────────────────────────────
# Quarantine helper (psycopg2 — Spark cannot INSERT JSONB)
# ─────────────────────────────────────────────────────────────────────────────

def quarantine_rows(rejected: list[dict], source_dataset: str, target_table: str, reason: str, detail: str):
    if not rejected:
        return
    conn = psycopg2.connect(
        host=DBConfig.HOST, port=DBConfig.PORT,
        dbname=DBConfig.DB, user=DBConfig.USER, password=DBConfig.PASSWORD
    )
    try:
        with conn.cursor() as cur:
            for i, row in enumerate(rejected):
                cur.execute(
                    """INSERT INTO staging_quarantine
                       (source_dataset, target_table, raw_row_json,
                        reject_reason, reject_detail, source_row_number,
                        spark_job_run_id, resolution)
                       VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s, 'PENDING')""",
                    (source_dataset, target_table,
                     json.dumps(row), reason, detail, i + 2, RUN_ID)
                )
        conn.commit()
        logger.info("Quarantined %d rows from %s.", len(rejected), source_dataset)
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Generic JDBC writer
# ─────────────────────────────────────────────────────────────────────────────

def jdbc_write(df: DataFrame, table: str, mode: str = "append") -> int:
    count = df.count()
    if count == 0:
        logger.info("No rows to write for %s.", table)
        return 0
    (
        df.write
        .format("jdbc")
        .option("url",           DBConfig.jdbc_url())
        .option("dbtable",       table)
        .option("user",          DBConfig.USER)
        .option("password",      DBConfig.PASSWORD)
        .option("driver",        "org.postgresql.Driver")
        .option("batchsize",     "1000")
        .option("numPartitions", "2")
        .mode(mode)
        .save()
    )
    logger.info("Wrote %d rows → %s (mode=%s).", count, table, mode)
    return count


# ─────────────────────────────────────────────────────────────────────────────
# Individual loaders
# ─────────────────────────────────────────────────────────────────────────────

def load_suppliers(spark: SparkSession):
    logger.info("── Loading suppliers ──")
    df = spark.read.option("header", True).option("inferSchema", False).csv(DataPaths.SUPPLIERS)
    # Dedup on supplier_id
    deduped = df.dropDuplicates(["supplier_id"]).filter(F.col("supplier_id").isNotNull())
    # Cast types
    clean = (
        deduped
        .withColumn("lead_time_days",    F.col("lead_time_days").cast(IntegerType()))
        .withColumn("reliability_score", F.col("reliability_score").cast(DoubleType()))
        .withColumn("is_active",         F.col("is_active").isin("TRUE","true","1").cast(BooleanType()))
    )
    jdbc_write(clean, "suppliers", mode="overwrite")


def load_stores(spark: SparkSession):
    logger.info("── Loading stores ──")
    df = spark.read.option("header", True).option("inferSchema", False).csv(DataPaths.STORES)
    clean = (
        df.dropDuplicates(["store_id"])
        .filter(F.col("store_id").isNotNull())
        .withColumn("warehouse_capacity_units", F.col("warehouse_capacity_units").cast(IntegerType()))
        .withColumn("is_active", F.col("is_active").isin("TRUE","true","1").cast(BooleanType()))
        # Replace empty region with NULL
        .withColumn("region", F.when(F.col("region") == "", F.lit(None)).otherwise(F.col("region")))
    )
    jdbc_write(clean, "stores", mode="overwrite")


def load_products(spark: SparkSession):
    logger.info("── Loading products ──")
    df = spark.read.option("header", True).option("inferSchema", False).csv(DataPaths.PRODUCTS)
    clean = (
        df.dropDuplicates(["product_id"])
        .filter(F.col("product_id").isNotNull())
        .withColumn("unit_cost",        F.col("unit_cost").cast(DoubleType()))       # NULL allowed
        .withColumn("unit_price",       F.col("unit_price").cast(DoubleType()))
        .withColumn("reorder_point",    F.col("reorder_point").cast(IntegerType()))
        .withColumn("reorder_quantity", F.col("reorder_quantity").cast(IntegerType()))
        .withColumn("weight_kg",        F.col("weight_kg").cast(DoubleType()))
        .withColumn("is_active",        F.col("is_active").isin("TRUE","true","1").cast(BooleanType()))
        .filter(F.col("unit_price").isNotNull() & (F.col("unit_price") > 0))
    )
    jdbc_write(clean, "products", mode="overwrite")


def load_orders(spark: SparkSession):
    logger.info("── Loading orders ──")
    df = spark.read.option("header", True).option("inferSchema", False).csv(DataPaths.ORDERS)

    VALID_STATUS = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]

    # Split: valid vs quarantine
    valid   = df.filter(F.col("status").isin(VALID_STATUS))
    invalid = df.filter(~F.col("status").isin(VALID_STATUS))

    if not invalid.rdd.isEmpty():
        bad_rows = [row.asDict() for row in invalid.collect()]
        quarantine_rows(bad_rows, "transactions/orders.csv", "orders",
                        "INVALID_ENUM_VALUE", f"status not in {VALID_STATUS}")

    clean = (
        valid.dropDuplicates(["order_id"])
        .filter(F.col("order_id").isNotNull())
        .withColumn("order_date",              F.to_date(F.col("order_date")))
        .withColumn("expected_delivery_date",  F.to_date(F.col("expected_delivery_date")))
        .withColumn("actual_delivery_date",
            F.when(F.col("actual_delivery_date") == "", F.lit(None))
             .otherwise(F.to_date(F.col("actual_delivery_date")))
        )
        .withColumn("quantity_ordered", F.col("quantity_ordered").cast(IntegerType()))
        .withColumn("unit_price",       F.col("unit_price").cast(DoubleType()))
        .withColumn("total_amount",     F.col("total_amount").cast(DoubleType()))
    )
    jdbc_write(clean, "orders")


def load_shipments(spark: SparkSession):
    logger.info("── Loading shipments ──")
    df = spark.read.option("header", True).option("inferSchema", False).csv(DataPaths.SHIPMENTS)

    # Flag negative freight_cost rows
    invalid = df.filter(F.col("freight_cost").cast(DoubleType()) < 0)
    if not invalid.rdd.isEmpty():
        bad_rows = [row.asDict() for row in invalid.collect()]
        quarantine_rows(bad_rows, "transactions/shipments.csv", "shipments",
                        "NEGATIVE_NUMERIC", "freight_cost < 0")

    clean = (
        df.filter(
            F.col("shipment_id").isNotNull() &
            (F.col("freight_cost").cast(DoubleType()) >= 0)
        )
        .dropDuplicates(["shipment_id"])
        .withColumn("shipment_date",     F.to_date(F.col("shipment_date")))
        .withColumn("estimated_arrival", F.to_date(F.col("estimated_arrival")))
        .withColumn("actual_arrival",
            F.when(F.col("actual_arrival") == "", F.lit(None))
             .otherwise(F.to_timestamp(F.col("actual_arrival")))
        )
        .withColumn("quantity_shipped",  F.col("quantity_shipped").cast(IntegerType()))
        .withColumn("freight_cost",      F.col("freight_cost").cast(DoubleType()))
    )
    jdbc_write(clean, "shipments")


def load_stock_movements(spark: SparkSession):
    logger.info("── Loading stock_movements ──")
    df = spark.read.option("header", True).option("inferSchema", False).csv(DataPaths.STOCK_MOVEMENTS)

    # Quarantine zero-quantity rows
    invalid = df.filter(F.col("quantity").cast(IntegerType()) == 0)
    if not invalid.rdd.isEmpty():
        bad_rows = [row.asDict() for row in invalid.collect()]
        quarantine_rows(bad_rows, "transactions/stock_movements.csv", "stock_movements",
                        "ZERO_QUANTITY", "quantity = 0 is not allowed")

    clean = (
        df.filter(
            F.col("movement_id").isNotNull() &
            (F.col("quantity").cast(IntegerType()) != 0)
        )
        .dropDuplicates(["movement_id"])
        .withColumn("quantity",      F.col("quantity").cast(IntegerType()))
        .withColumn("movement_date", F.to_timestamp(F.col("movement_date")))
        .withColumn("store_id",
            F.when(F.col("store_id") == "", F.lit(None)).otherwise(F.col("store_id"))
        )
    )
    jdbc_write(clean, "stock_movements")


def load_historical_sales(spark: SparkSession):
    logger.info("── Loading historical_sales ──")
    df = spark.read.option("header", True).option("inferSchema", False).csv(DataPaths.HISTORICAL_SALES)

    invalid = df.filter(F.col("quantity_sold").cast(IntegerType()) <= 0)
    if not invalid.rdd.isEmpty():
        bad_rows = [row.asDict() for row in invalid.collect()]
        quarantine_rows(bad_rows, "historical/historical_sales.csv", "historical_sales",
                        "ZERO_QUANTITY", "quantity_sold must be > 0")

    clean = (
        df.filter(
            F.col("sale_id").isNotNull() &
            (F.col("quantity_sold").cast(IntegerType()) > 0)
        )
        .dropDuplicates(["sale_id"])
        .withColumn("sale_date",     F.to_date(F.col("sale_date")))
        .withColumn("quantity_sold", F.col("quantity_sold").cast(IntegerType()))
        .withColumn("unit_price",    F.col("unit_price").cast(DoubleType()))
        .withColumn("discount_pct",  F.col("discount_pct").cast(DoubleType()))
        .withColumn("total_revenue", F.col("total_revenue").cast(DoubleType()))
    )
    jdbc_write(clean, "historical_sales")


def load_stockout_risk(spark: SparkSession):
    logger.info("── Loading inventory_demand_stockout_risk ──")
    df = spark.read.option("header", True).option("inferSchema", False).csv(DataPaths.STOCKOUT_RISK)
    clean = (
        df.dropDuplicates(["risk_id"])
        .filter(F.col("risk_id").isNotNull())
        .withColumn("snapshot_date",           F.to_date(F.col("snapshot_date")))
        .withColumn("current_stock_units",     F.col("current_stock_units").cast(IntegerType()))
        .withColumn("avg_daily_demand_units",  F.col("avg_daily_demand_units").cast(DoubleType()))
        .withColumn("days_of_stock_remaining",
            F.when(F.col("days_of_stock_remaining") == "", F.lit(None))
             .otherwise(F.col("days_of_stock_remaining").cast(DoubleType()))
        )
        .withColumn("reorder_point_units",     F.col("reorder_point_units").cast(IntegerType()))
        .withColumn("stockout_risk_flag",
            F.col("stockout_risk_flag").isin("TRUE","true","1").cast(BooleanType())
        )
        .withColumn("overstock_flag",
            F.col("overstock_flag").isin("TRUE","true","1").cast(BooleanType())
        )
        .withColumn("last_restocked_date",
            F.when(F.col("last_restocked_date") == "", F.lit(None))
             .otherwise(F.to_date(F.col("last_restocked_date")))
        )
    )
    jdbc_write(clean, "inventory_demand_stockout_risk")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main(mode: str):
    logger.info("LoadReferenceData START  mode=%s  run_id=%s", mode, RUN_ID)
    spark = get_batch_session("LoadReferenceData")

    if mode in ("full", "reference-only"):
        load_suppliers(spark)
        load_stores(spark)
        load_products(spark)

    if mode == "full":
        load_orders(spark)
        load_shipments(spark)
        load_stock_movements(spark)

    if mode in ("full", "historical-only"):
        load_historical_sales(spark)
        load_stockout_risk(spark)

    logger.info("LoadReferenceData COMPLETE  run_id=%s", RUN_ID)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["full", "reference-only", "historical-only"],
        default="full",
    )
    args = parser.parse_args()
    main(args.mode)
