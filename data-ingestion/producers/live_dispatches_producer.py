"""
LiveDispatchesProducer
─────────────────────────────────────────────────────────────────────────────
Reads data/raw/streaming/live_dispatches.csv row by row and publishes each row
as a JSON event to the Kafka topic: inventory-live-dispatches

Each event represents a warehouse dispatch or inter-store stock transfer:
  RESTOCK | TRANSFER | EMERGENCY_REPLENISH | RETURN_TO_SUPPLIER

Partition key: from_store_id
  → All dispatch events originating from the same warehouse go to the
    same partition. This allows Spark to compute live outbound stock
    per warehouse without cross-partition state.

Validation note: from_store_id ≠ to_store_id is enforced in the Pydantic
schema (a store cannot dispatch stock to itself).

Usage:
    python live_dispatches_producer.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from config.settings import KafkaConfig, DataPaths
from data_ingestion.schemas.events import LiveDispatchEvent
from data_ingestion.producers.base_producer import BaseProducer


class LiveDispatchesProducer(BaseProducer):

    @property
    def topic(self) -> str:
        return KafkaConfig.TOPIC_LIVE_DISPATCHES

    @property
    def schema_class(self):
        return LiveDispatchEvent

    @property
    def csv_path(self) -> str:
        return DataPaths.LIVE_DISPATCHES

    @property
    def producer_name(self) -> str:
        return "LiveDispatchesProducer"

    def _message_key(self, event: LiveDispatchEvent) -> str:
        # Partition by from_store_id for per-warehouse outbound stock tracking.
        return event.from_store_id


if __name__ == "__main__":
    LiveDispatchesProducer().run()
