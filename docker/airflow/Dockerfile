# =============================================================================
# Airflow image for Smart Inventory System
# Base: apache/airflow 2.9.1
# Adds: apache-airflow-providers-apache-spark, psycopg2, kafka-python
# =============================================================================

FROM apache/airflow:2.9.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.8.0 \
    apache-airflow-providers-postgres==5.10.2 \
    psycopg2-binary==2.9.9 \
    kafka-python==2.0.2 \
    python-dotenv==1.0.1
