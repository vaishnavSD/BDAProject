"""
LiveOrdersProducer
─────────────────────────────────────────────────────────────────────────────
Reads data/raw/streaming/live_orders.csv row by row and publishes each row
as a JSON event to the Kafka topic: inventory-live-orders

Each event represents a real-time order placement from one of four channels:
  WEB | MOBILE | B2B_PORTAL | EDI

Partition key: product_id
  → All order events for the same product land on the same partition,
    allowing Spark Structured Streaming to maintain per-product state
    without cross-partition shuffles.

Usage:
    python live_orders_producer.py
    PRODUCER_DELAY_SECONDS=1 python live_orders_producer.py
    PRODUCER_LOOP=TRUE python live_orders_producer.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from config.settings import KafkaConfig, DataPaths
from data_ingestion.schemas.events import LiveOrderEvent
from data_ingestion.producers.base_producer import BaseProducer


class LiveOrdersProducer(BaseProducer):

    @property
    def topic(self) -> str:
        return KafkaConfig.TOPIC_LIVE_ORDERS

    @property
    def schema_class(self):
        return LiveOrderEvent

    @property
    def csv_path(self) -> str:
        return DataPaths.LIVE_ORDERS

    @property
    def producer_name(self) -> str:
        return "LiveOrdersProducer"

    def _message_key(self, event: LiveOrderEvent) -> str:
        # Partition by product_id so all orders for the same product
        # are processed in order by the same Spark task.
        return event.product_id


if __name__ == "__main__":
    LiveOrdersProducer().run()
