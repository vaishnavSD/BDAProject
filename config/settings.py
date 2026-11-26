"""
Centralised configuration loader.
All modules import from here — never read os.environ directly in business code.
"""

import os
from dotenv import load_dotenv

# Load .env from project root (two levels up from this file)
_env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=_env_path, override=False)


# ── PostgreSQL ────────────────────────────────────────────────────────────────
class DBConfig:
    HOST     = os.getenv("POSTGRES_HOST", "localhost")
    PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
    DB       = os.getenv("POSTGRES_DB", "smart_inventory")
    USER     = os.getenv("POSTGRES_USER", "inventory")
    PASSWORD = os.getenv("POSTGRES_PASSWORD", "inventory")

    # SQLAlchemy connection string
    @classmethod
    def url(cls) -> str:
        return f"postgresql+psycopg2://{cls.USER}:{cls.PASSWORD}@{cls.HOST}:{cls.PORT}/{cls.DB}"

    # Spark JDBC connection string
    @classmethod
    def jdbc_url(cls) -> str:
        return f"jdbc:postgresql://{cls.HOST}:{cls.PORT}/{cls.DB}"

    # Spark JDBC properties dict
    @classmethod
    def jdbc_properties(cls) -> dict:
        return {
            "user":     cls.USER,
            "password": cls.PASSWORD,
            "driver":   "org.postgresql.Driver",
        }


# ── Kafka ─────────────────────────────────────────────────────────────────────
class KafkaConfig:
    BOOTSTRAP_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    TOPIC_LIVE_ORDERS     = os.getenv("KAFKA_TOPIC_LIVE_ORDERS",     "inventory-live-orders")
    TOPIC_LIVE_SHIPMENTS  = os.getenv("KAFKA_TOPIC_LIVE_SHIPMENTS",  "inventory-live-shipments")
    TOPIC_LIVE_DISPATCHES = os.getenv("KAFKA_TOPIC_LIVE_DISPATCHES", "inventory-live-dispatches")
    TOPIC_ALERTS          = os.getenv("KAFKA_TOPIC_ALERTS",          "inventory-alerts")
    TOPIC_DLQ             = os.getenv("KAFKA_TOPIC_DLQ",             "inventory-dlq")

    # Producer defaults
    PRODUCER_CONFIG = {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "acks": "all",                   # wait for all in-sync replicas
        "retries": 3,
        "max_block_ms": 10_000,
        "value_serializer": None,        # set per-producer (JSON bytes)
    }

    # Consumer defaults (used by test consumers)
    CONSUMER_CONFIG = {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "auto_offset_reset": "earliest",
        "enable_auto_commit": True,
        "group_id": "inventory-consumer-group",
    }


# ── Spark ─────────────────────────────────────────────────────────────────────
class SparkConfig:
    MASTER_URL      = os.getenv("SPARK_MASTER_URL", "spark://localhost:7077")
    APP_NAME        = os.getenv("SPARK_APP_NAME",   "SmartInventorySystem")
    CHECKPOINT_DIR  = os.getenv("SPARK_CHECKPOINT_DIR", "/opt/spark-checkpoints")

    # Kafka JAR packages needed for Structured Streaming
    KAFKA_PACKAGES  = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

    # Streaming trigger interval (seconds)
    STREAMING_TRIGGER_SECONDS = 10

    # Streaming watermark (minutes) — late data tolerance
    WATERMARK_MINUTES = 5


# ── Data Paths ────────────────────────────────────────────────────────────────
class DataPaths:
    BASE        = os.getenv("RAW_DATA_BASE",          "./data/raw")
    REFERENCE   = os.getenv("REFERENCE_DATA_PATH",    "./data/raw/reference")
    TRANSACTIONS= os.getenv("TRANSACTION_DATA_PATH",  "./data/raw/transactions")
    HISTORICAL  = os.getenv("HISTORICAL_DATA_PATH",   "./data/raw/historical")
    STREAMING   = os.getenv("STREAMING_DATA_PATH",    "./data/raw/streaming")

    SUPPLIERS   = f"{REFERENCE}/suppliers.csv"
    STORES      = f"{REFERENCE}/stores.csv"
    PRODUCTS    = f"{REFERENCE}/products.csv"

    ORDERS          = f"{TRANSACTIONS}/orders.csv"
    SHIPMENTS       = f"{TRANSACTIONS}/shipments.csv"
    STOCK_MOVEMENTS = f"{TRANSACTIONS}/stock_movements.csv"

    HISTORICAL_SALES      = f"{HISTORICAL}/historical_sales.csv"
    STOCKOUT_RISK         = f"{HISTORICAL}/inventory_demand_stockout_risk.csv"

    LIVE_ORDERS     = f"{STREAMING}/live_orders.csv"
    LIVE_SHIPMENTS  = f"{STREAMING}/live_shipments.csv"
    LIVE_DISPATCHES = f"{STREAMING}/live_dispatches.csv"


# ── Producer Settings ─────────────────────────────────────────────────────────
class ProducerConfig:
    DELAY_SECONDS = float(os.getenv("PRODUCER_DELAY_SECONDS", "0.5"))
    LOOP          = os.getenv("PRODUCER_LOOP", "FALSE").upper() == "TRUE"
