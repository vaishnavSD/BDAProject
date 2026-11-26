"""
dag_daily_inventory.py
─────────────────────────────────────────────────────────────────────────────
DAG: daily_inventory_pipeline
Schedule: daily at 02:00 UTC

Task dependency chain (enforces write-before-read ordering):

  start
    │
    ▼
  daily_stock_snapshot          ← reads stock_movements
    │
    ▼
  overstock_detection            ← reads batch_daily_stock_snapshot
    │
    ▼
  end

Both jobs are idempotent — safe to retry and re-run for the same date.
The execution_date passed by Airflow ({{ ds }}) is yesterday's date,
matching the convention that nightly jobs process the previous day.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

SPARK_CONN = "spark_default"
JDBC_JAR   = "/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
APPS_DIR   = "/opt/airflow/batch"

default_args = {
    "owner":            "inventory-team",
    "depends_on_past":  True,         # don't run if yesterday failed
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout":timedelta(hours=1),
    "email_on_failure": False,
}

with DAG(
    dag_id="daily_inventory_pipeline",
    description="Daily stock snapshot and overstock detection",
    schedule_interval="0 2 * * *",    # 02:00 UTC every day
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["batch", "daily"],
    default_args=default_args,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # ── Task 1: Daily Stock Snapshot ──────────────────────────────────────────
    # Uses {{ ds }} = execution date (YYYY-MM-DD) passed by Airflow.
    # Airflow runs at 02:00 on day D, processes movements from day D-1.
    daily_snapshot = SparkSubmitOperator(
        task_id="daily_stock_snapshot",
        conn_id=SPARK_CONN,
        application=f"{APPS_DIR}/jobs/daily_stock_snapshot.py",
        application_args=["--date", "{{ ds }}"],
        jars=JDBC_JAR,
        name="DailyStockSnapshot-{{ ds }}",
        conf={
            "spark.sql.shuffle.partitions": "8",
            "spark.driver.memory":          "1g",
            "spark.executor.memory":        "1g",
        },
        verbose=False,
    )

    # ── Task 2: Overstock Detection ───────────────────────────────────────────
    # Depends on daily_snapshot — reads from batch_daily_stock_snapshot.
    overstock = SparkSubmitOperator(
        task_id="overstock_detection",
        conn_id=SPARK_CONN,
        application=f"{APPS_DIR}/jobs/overstock_detection.py",
        application_args=["--date", "{{ ds }}"],
        jars=JDBC_JAR,
        name="OverstockDetection-{{ ds }}",
        conf={
            "spark.sql.shuffle.partitions": "8",
            "spark.driver.memory":          "1g",
        },
        verbose=False,
    )

    start >> daily_snapshot >> overstock >> end
