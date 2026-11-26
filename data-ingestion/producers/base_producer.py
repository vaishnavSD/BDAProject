"""
BaseProducer
─────────────────────────────────────────────────────────────────────────────
Shared logic for all three CSV-replay Kafka producers.

Responsibilities:
  - Connect to Kafka with retry logic.
  - Read a CSV file row by row.
  - Validate each row against its Pydantic schema.
  - Publish valid rows to the target Kafka topic as JSON bytes.
  - Publish invalid rows to the Dead Letter Queue (DLQ) topic.
  - Log progress, delivery confirmations, and errors.
  - Respect configurable publish delay between rows (simulates real time).
  - Support optional looping (replay CSV endlessly until stopped).
"""

import csv
import json
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Type

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from pydantic import BaseModel, ValidationError

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from config.settings import KafkaConfig, ProducerConfig
from data_ingestion.schemas.events import DLQEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class BaseProducer(ABC):
    """
    Abstract base class for all CSV-replay Kafka producers.
    Subclasses must implement: topic, schema_class, csv_path, producer_name.
    """

    MAX_CONNECT_RETRIES = 10
    CONNECT_RETRY_DELAY = 5   # seconds between connection attempts

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self._producer: KafkaProducer | None = None
        self._dlq_producer: KafkaProducer | None = None
        self._stats = {
            "total_rows":    0,
            "published":     0,
            "rejected":      0,
            "dlq_sent":      0,
        }

    # ── Abstract interface ────────────────────────────────────────────────────

    @property
    @abstractmethod
    def topic(self) -> str:
        """Kafka topic to publish valid events to."""

    @property
    @abstractmethod
    def schema_class(self) -> Type[BaseModel]:
        """Pydantic model class for row validation."""

    @property
    @abstractmethod
    def csv_path(self) -> str:
        """Absolute or relative path to the source CSV file."""

    @property
    @abstractmethod
    def producer_name(self) -> str:
        """Human-readable name shown in logs."""

    # ── Kafka connection ──────────────────────────────────────────────────────

    def _connect(self) -> None:
        """Connect to Kafka with retry; raises after MAX_CONNECT_RETRIES."""
        for attempt in range(1, self.MAX_CONNECT_RETRIES + 1):
            try:
                self.logger.info(
                    "Connecting to Kafka at %s (attempt %d/%d)...",
                    KafkaConfig.BOOTSTRAP_SERVERS, attempt, self.MAX_CONNECT_RETRIES
                )
                self._producer = KafkaProducer(
                    bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    acks="all",
                    retries=3,
                    max_block_ms=10_000,
                )
                self._dlq_producer = KafkaProducer(
                    bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    acks=1,
                    retries=2,
                )
                self.logger.info("Connected to Kafka successfully.")
                return
            except NoBrokersAvailable:
                self.logger.warning(
                    "Kafka not reachable. Retrying in %ds...", self.CONNECT_RETRY_DELAY
                )
                time.sleep(self.CONNECT_RETRY_DELAY)
        raise RuntimeError(
            f"Could not connect to Kafka after {self.MAX_CONNECT_RETRIES} attempts."
        )

    # ── DLQ publish ───────────────────────────────────────────────────────────

    def _send_to_dlq(self, raw_row: dict, error_message: str) -> None:
        dlq_event = DLQEvent(
            source_topic=self.topic,
            source_file=self.csv_path,
            raw_row=raw_row,
            error_message=error_message,
            failed_at=datetime.utcnow().isoformat(),
        )
        try:
            self._dlq_producer.send(
                KafkaConfig.TOPIC_DLQ,
                value=dlq_event.to_kafka_dict(),
            )
            self._stats["dlq_sent"] += 1
        except KafkaError as e:
            self.logger.error("Failed to send to DLQ: %s", e)

    # ── Delivery callback ─────────────────────────────────────────────────────

    def _on_send_success(self, record_metadata):
        self.logger.debug(
            "✓ topic=%s partition=%d offset=%d",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )

    def _on_send_error(self, exc):
        self.logger.error("✗ Kafka delivery error: %s", exc)

    # ── Main run loop ─────────────────────────────────────────────────────────

    def run(self) -> None:
        """Read CSV, validate, publish. Loops if ProducerConfig.LOOP is True."""
        self._connect()
        self.logger.info(
            "%s started. topic=%s file=%s delay=%.2fs loop=%s",
            self.producer_name,
            self.topic,
            self.csv_path,
            ProducerConfig.DELAY_SECONDS,
            ProducerConfig.LOOP,
        )

        iteration = 0
        try:
            while True:
                iteration += 1
                if ProducerConfig.LOOP:
                    self.logger.info("── Loop iteration %d ──", iteration)

                self._publish_csv()

                if not ProducerConfig.LOOP:
                    break
                # Small pause between loops to avoid flooding Kafka in test mode
                time.sleep(2)

        finally:
            self._producer.flush()
            self._producer.close()
            self._dlq_producer.flush()
            self._dlq_producer.close()
            self._log_summary()

    def _publish_csv(self) -> None:
        """Read the CSV once and publish every valid row."""
        with open(self.csv_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row_num, raw_row in enumerate(reader, start=2):   # start=2: row 1 is header
                self._stats["total_rows"] += 1

                # Strip whitespace from all values
                clean_row = {k: v.strip() for k, v in raw_row.items()}

                # Validate
                try:
                    event = self.schema_class(**clean_row)
                except (ValidationError, ValueError, Exception) as exc:
                    self._stats["rejected"] += 1
                    self.logger.warning(
                        "Row %d rejected: %s | row=%s", row_num, exc, clean_row
                    )
                    self._send_to_dlq(clean_row, str(exc))
                    continue

                # Publish
                try:
                    self._producer.send(
                        self.topic,
                        value=event.to_kafka_dict(),
                        key=self._message_key(event).encode("utf-8"),
                    ).add_callback(self._on_send_success).add_errback(self._on_send_error)
                    self._stats["published"] += 1
                except KafkaError as exc:
                    self.logger.error("Kafka send error on row %d: %s", row_num, exc)
                    self._stats["rejected"] += 1

                if ProducerConfig.DELAY_SECONDS > 0:
                    time.sleep(ProducerConfig.DELAY_SECONDS)

    def _message_key(self, event: BaseModel) -> str:
        """
        Kafka message key — used for partition routing.
        Default: product_id. Subclasses may override.
        """
        return getattr(event, "product_id", "unknown")

    # ── Logging ───────────────────────────────────────────────────────────────

    def _log_summary(self) -> None:
        s = self._stats
        self.logger.info(
            "%s finished. total=%d  published=%d  rejected=%d  dlq=%d",
            self.producer_name,
            s["total_rows"], s["published"], s["rejected"], s["dlq_sent"],
        )
