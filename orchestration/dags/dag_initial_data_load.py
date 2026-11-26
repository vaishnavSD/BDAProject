"""
dag_initial_data_load.py
─────────────────────────────────────────────────────────────────────────────
DAG: initial_data_load
Schedule: @once  (manual trigger only)
Purpose:  Loads all 8 CSVs into PostgreSQL on first system boot.
          This DAG must succeed before any other DAG runs.

Task dependency:
  load_reference_data_full
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

SPARK_CONN   = "spark_default"
JDBC_JAR     = "/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
APPS_DIR     = "/opt/airflow/batch"

default_args = {
    "owner":            "inventory-team",
    "depends_on_past":  False,
    "retries":          1,
}

with DAG(
    dag_id="initial_data_load",
    description="One-time load of all CSV reference and historical data into PostgreSQL",
    schedule_interval=None,          # triggered manually
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["batch", "init"],
    default_args=default_args,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    load_all = SparkSubmitOperator(
        task_id="load_all_csv_data",
        conn_id=SPARK_CONN,
        application=f"{APPS_DIR}/jobs/load_reference_data.py",
        application_args=["--mode", "full"],
        jars=JDBC_JAR,
        name="InitialDataLoad",
        conf={
            "spark.sql.shuffle.partitions": "8",
            "spark.driver.memory":          "1g",
        },
        verbose=True,
    )

    start >> load_all >> end
