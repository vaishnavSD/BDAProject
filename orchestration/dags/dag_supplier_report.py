"""
dag_supplier_report.py
─────────────────────────────────────────────────────────────────────────────
DAG: weekly_supplier_performance
Schedule: every Monday at 04:00 UTC

Computes supplier on-time rate, fill rate, and avg lead time for the
past 7 days. Raises SUPPLIER_DELAY alerts for under-performing suppliers.

Task dependency:
  start → supplier_performance → end
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

SPARK_CONN = "spark_default"
JDBC_JAR   = "/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
APPS_DIR   = "/opt/airflow/batch"

default_args = {
    "owner":             "inventory-team",
    "depends_on_past":   False,
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="weekly_supplier_performance",
    description="Weekly supplier on-time rate and fill-rate report",
    schedule_interval="0 4 * * 1",    # 04:00 UTC every Monday
    start_date=datetime(2025, 1, 6),  # first Monday of 2025
    catchup=False,
    tags=["batch", "weekly", "supplier"],
    default_args=default_args,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # period_start = 7 days before execution date
    # period_end   = execution date (yesterday from the weekly run perspective)
    supplier_perf = SparkSubmitOperator(
        task_id="supplier_performance",
        conn_id=SPARK_CONN,
        application=f"{APPS_DIR}/jobs/supplier_performance.py",
        application_args=[
            "--start", "{{ macros.ds_add(ds, -7) }}",
            "--end",   "{{ ds }}",
        ],
        jars=JDBC_JAR,
        name="SupplierPerformance-{{ ds }}",
        conf={
            "spark.sql.shuffle.partitions": "8",
            "spark.driver.memory":          "1g",
        },
        verbose=False,
    )

    start >> supplier_perf >> end
