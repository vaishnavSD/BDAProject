"""
LiveShipmentsProducer
─────────────────────────────────────────────────────────────────────────────
Reads data/raw/streaming/live_shipments.csv row by row and publishes each row
as a JSON event to the Kafka topic: inventory-live-shipments

Each event represents a real-time status update from a carrier:
  DISPATCHED → IN_TRANSIT → OUT_FOR_DELIVERY → DELIVERED | DELAYED

Partition key: shipment_id
  → All status updates for the same physical shipment go to the same
    partition, preserving event order per shipment.

Business logic handled downstream (in Spark):
  - When status = DELIVERED, update stream_live_shipments.actual_arrival.
  - When status = DELAYED, generate an alert in the alerts table.

Usage:
    python live_shipments_producer.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from config.settings import KafkaConfig, DataPaths
from data_ingestion.schemas.events import LiveShipmentEvent
from data_ingestion.producers.base_producer import BaseProducer


class LiveShipmentsProducer(BaseProducer):

    @property
    def topic(self) -> str:
        return KafkaConfig.TOPIC_LIVE_SHIPMENTS

    @property
    def schema_class(self):
        return LiveShipmentEvent

    @property
    def csv_path(self) -> str:
        return DataPaths.LIVE_SHIPMENTS

    @property
    def producer_name(self) -> str:
        return "LiveShipmentsProducer"

    def _message_key(self, event: LiveShipmentEvent) -> str:
        # Partition by shipment_id: preserves state transition order per shipment.
        return event.shipment_id


if __name__ == "__main__":
    LiveShipmentsProducer().run()
