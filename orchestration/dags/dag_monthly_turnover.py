"""
dag_monthly_turnover.py
─────────────────────────────────────────────────────────────────────────────
DAG: monthly_inventory_turnover
Schedule: 1st of every month at 05:00 UTC

Computes inventory turnover ratio per product per store per category
for the previous month. Flags slow movers (turnover < 2) with OVERSTOCK alerts.

Task dependency:
  start → inventory_turnover → end
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
    "retry_delay":       timedelta(minutes=15),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="monthly_inventory_turnover",
    description="Monthly inventory turnover ratio and slow-mover detection",
    schedule_interval="0 5 1 * *",    # 05:00 UTC on 1st of every month
    start_date=datetime(2025, 2, 1),
    catchup=False,
    tags=["batch", "monthly"],
    default_args=default_args,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # {{ macros.ds_format(ds, "%Y-%m-%d", "%Y-%m") }} extracts YYYY-MM
    # from the execution_date, which on the 1st is the current month,
    # so we subtract 1 month using a custom macro approach.
    # Simpler: pass previous_month as a computed string.
    turnover = SparkSubmitOperator(
        task_id="inventory_turnover",
        conn_id=SPARK_CONN,
        application=f"{APPS_DIR}/jobs/inventory_turnover.py",
        # Pass previous month: execution_date is 2025-02-01, so month=2025-01
        application_args=[
            "--month",
            "{{ (execution_date.replace(day=1) - macros.timedelta(days=1)).strftime('%Y-%m') }}",
        ],
        jars=JDBC_JAR,
        name="InventoryTurnover-{{ ds }}",
        conf={
            "spark.sql.shuffle.partitions": "16",
            "spark.driver.memory":          "1g",
            "spark.executor.memory":        "2g",
        },
        verbose=False,
    )

    start >> turnover >> end
