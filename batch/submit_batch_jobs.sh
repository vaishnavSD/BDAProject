#!/bin/bash
# =============================================================================
# submit_batch_jobs.sh
# Manual batch job submission to Spark cluster (bypass Airflow for testing).
# Run inside inventory_spark_master container:
#   docker exec -it inventory_spark_master bash /opt/spark-apps/batch/submit_batch_jobs.sh <job> [args]
#
# Examples:
#   bash submit_batch_jobs.sh load_reference   --mode full
#   bash submit_batch_jobs.sh daily_snapshot   --date 2024-12-31
#   bash submit_batch_jobs.sh overstock        --date 2024-12-31
#   bash submit_batch_jobs.sh supplier_perf    --start 2024-12-24 --end 2024-12-31
#   bash submit_batch_jobs.sh turnover         --month 2024-12
# =============================================================================

SPARK_MASTER="spark://spark-master:7077"
JDBC_JAR="/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
APPS_DIR="/opt/spark-apps/batch"

JOB=${1:-"help"}
shift   # remaining args passed to the Python job

submit() {
    local name=$1
    local script=$2
    echo "==> Submitting $name with args: $@"
    spark-submit \
        --master "$SPARK_MASTER" \
        --deploy-mode client \
        --jars "$JDBC_JAR" \
        --conf "spark.sql.shuffle.partitions=8" \
        --conf "spark.driver.memory=1g" \
        --name "$name" \
        "$script" "$@"
}

case "$JOB" in
  load_reference)
    submit "LoadReferenceData" "$APPS_DIR/jobs/load_reference_data.py" "$@"
    ;;
  daily_snapshot)
    submit "DailyStockSnapshot" "$APPS_DIR/jobs/daily_stock_snapshot.py" "$@"
    ;;
  overstock)
    submit "OverstockDetection" "$APPS_DIR/jobs/overstock_detection.py" "$@"
    ;;
  supplier_perf)
    submit "SupplierPerformance" "$APPS_DIR/jobs/supplier_performance.py" "$@"
    ;;
  turnover)
    submit "InventoryTurnover" "$APPS_DIR/jobs/inventory_turnover.py" "$@"
    ;;
  all_daily)
    TODAY=$(date -d "yesterday" +%Y-%m-%d)
    submit "LoadReferenceData"  "$APPS_DIR/jobs/load_reference_data.py"  --mode reference-only
    submit "DailyStockSnapshot" "$APPS_DIR/jobs/daily_stock_snapshot.py" --date "$TODAY"
    submit "OverstockDetection" "$APPS_DIR/jobs/overstock_detection.py"  --date "$TODAY"
    ;;
  help|*)
    echo "Usage: $0 [load_reference|daily_snapshot|overstock|supplier_perf|turnover|all_daily] [job-args]"
    exit 1
    ;;
esac
