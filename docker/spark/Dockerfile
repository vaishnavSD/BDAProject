# =============================================================================
# Spark image for Smart Inventory System
# Base: apache/spark 3.5.1 (includes Hadoop 3, Python 3)
# Adds: PostgreSQL JDBC driver, kafka-python, psycopg2 for PySpark jobs
# =============================================================================

FROM apache/spark:3.5.1

USER root

# ── System packages ──────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# ── PostgreSQL JDBC driver (required by Spark JDBC write) ───────────────────
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -O /opt/spark/jars/postgresql-42.7.3.jar

# ── Python dependencies for PySpark jobs ────────────────────────────────────
COPY requirements.txt /tmp/spark-requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/spark-requirements.txt

# ── Entrypoint script to start master or worker ─────────────────────────────
COPY entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

USER spark

ENTRYPOINT ["/opt/spark/entrypoint.sh"]
