# =============================================================================
# Spark image for Smart Inventory System
# Base: bitnami/spark 3.5 (includes Hadoop 3, Python 3.11)
# Adds: PostgreSQL JDBC driver, kafka-python, psycopg2 for PySpark jobs
# =============================================================================

FROM bitnami/spark:3.5.1

USER root

# ── System packages ──────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ── PostgreSQL JDBC driver (required by Spark JDBC write) ───────────────────
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -O /opt/bitnami/spark/jars/postgresql-42.7.3.jar

# ── Kafka connector JAR (Structured Streaming kafka source) ─────────────────
# spark-sql-kafka package; pulled at runtime via --packages in submit command.
# Listed here for documentation; not bundled to keep image lean.

# ── Python dependencies for PySpark jobs ────────────────────────────────────
COPY requirements.txt /tmp/spark-requirements.txt
RUN pip install --no-cache-dir -r /tmp/spark-requirements.txt

USER 1001
