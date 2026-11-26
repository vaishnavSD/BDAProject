#!/bin/bash
# =============================================================================
# submit_jobs.sh
# Run inside the spark-master Docker container to submit all streaming jobs.
#
# Usage (from host):
#   docker exec -it inventory_spark_master bash /opt/spark-apps/streaming/submit_jobs.sh
#
# Or submit individually:
#   docker exec -it inventory_spark_master bash /opt/spark-apps/streaming/submit_jobs.sh stock_aggregator
# =============================================================================

SPARK_MASTER="spark://spark-master:7077"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
JDBC_JAR="/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
APPS_DIR="/opt/spark-apps"

JOB=${1:-"all"}

submit_job() {
    local name=$1
    local script=$2
    echo ""
    echo "==> Submitting: $name"
    spark-submit \
        --master "$SPARK_MASTER" \
        --deploy-mode client \
        --packages "$KAFKA_PACKAGE" \
        --jars "$JDBC_JAR" \
        --conf "spark.sql.shuffle.partitions=8" \
        --conf "spark.streaming.stopGracefullyOnShutdown=true" \
        --name "$name" \
        "$script" &
    echo "    PID: $!"
}

case "$JOB" in
  stock_aggregator)
    submit_job "StockLevelAggregator" "$APPS_DIR/streaming/jobs/stock_level_aggregator.py"
    ;;
  threshold_detector)
    submit_job "ThresholdAlertDetector" "$APPS_DIR/streaming/jobs/threshold_alert_detector.py"
    ;;
  shipment_monitor)
    submit_job "ShipmentDelayMonitor" "$APPS_DIR/streaming/jobs/shipment_delay_monitor.py"
    ;;
  all)
    submit_job "StockLevelAggregator"   "$APPS_DIR/streaming/jobs/stock_level_aggregator.py"
    sleep 5
    submit_job "ThresholdAlertDetector" "$APPS_DIR/streaming/jobs/threshold_alert_detector.py"
    sleep 5
    submit_job "ShipmentDelayMonitor"   "$APPS_DIR/streaming/jobs/shipment_delay_monitor.py"
    ;;
  *)
    echo "Unknown job: $JOB"
    echo "Usage: $0 [stock_aggregator|threshold_detector|shipment_monitor|all]"
    exit 1
    ;;
esac

echo ""
echo "Jobs submitted. Monitor at: http://localhost:8081"
wait
