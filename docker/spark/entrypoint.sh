#!/bin/bash
# Entrypoint: start Spark master or worker based on SPARK_MODE env var

set -e

SPARK_MODE="${SPARK_MODE:-master}"

if [ "$SPARK_MODE" = "master" ]; then
    echo "Starting Spark Master..."
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
        --host "${SPARK_MASTER_HOST:-spark-master}" \
        --port "${SPARK_MASTER_PORT:-7077}" \
        --webui-port "${SPARK_MASTER_WEBUI_PORT:-8081}"
elif [ "$SPARK_MODE" = "worker" ]; then
    echo "Starting Spark Worker connecting to ${SPARK_MASTER_URL:-spark://spark-master:7077} ..."
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
        --webui-port "${SPARK_WORKER_WEBUI_PORT:-8082}" \
        --memory "${SPARK_WORKER_MEMORY:-2G}" \
        --cores "${SPARK_WORKER_CORES:-2}" \
        "${SPARK_MASTER_URL:-spark://spark-master:7077}"
else
    echo "Unknown SPARK_MODE: $SPARK_MODE"
    exit 1
fi
