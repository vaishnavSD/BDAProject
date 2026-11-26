"""
consumer_verify.py
─────────────────────────────────────────────────────────────────────────────
Test consumer — reads from all three topics and prints messages to stdout.
Use this to confirm producers are working before starting Spark.

Usage:
    # Verify all three topics
    python consumer_verify.py

    # Verify one specific topic
    python consumer_verify.py --topic inventory-live-orders

    # Limit messages consumed
    python consumer_verify.py --max 20

Press Ctrl+C to stop.
"""

import sys
import os
import json
import argparse
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from config.settings import KafkaConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ConsumerVerify")


def consume(topics: list[str], max_messages: int | None = None):
    logger.info("Connecting to Kafka at %s...", KafkaConfig.BOOTSTRAP_SERVERS)
    try:
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=None,            # read without committing — verification only
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            consumer_timeout_ms=10_000,   # stop polling after 10s of silence
        )
    except NoBrokersAvailable:
        logger.error("Cannot connect to Kafka. Is the broker running?")
        sys.exit(1)

    logger.info("Listening on topics: %s", topics)
    logger.info("Press Ctrl+C to stop.\n")

    counts = {t: 0 for t in topics}
    total  = 0

    try:
        for msg in consumer:
            topic  = msg.topic
            offset = msg.offset
            value  = msg.value

            counts[topic] = counts.get(topic, 0) + 1
            total += 1

            print(
                f"  [{topic}] partition={msg.partition} offset={offset}\n"
                f"  {json.dumps(value, indent=4)}\n"
            )

            if max_messages and total >= max_messages:
                logger.info("Reached max_messages=%d. Stopping.", max_messages)
                break

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        logger.info("\nSummary:")
        for t, c in counts.items():
            logger.info("  %-40s  %d messages consumed", t, c)
        logger.info("  Total: %d", total)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka topic verification consumer")
    parser.add_argument(
        "--topic", type=str, default=None,
        help="Single topic to consume. Defaults to all three inventory topics."
    )
    parser.add_argument(
        "--max", type=int, default=None, dest="max_messages",
        help="Stop after this many messages."
    )
    args = parser.parse_args()

    if args.topic:
        topics_to_consume = [args.topic]
    else:
        topics_to_consume = [
            KafkaConfig.TOPIC_LIVE_ORDERS,
            KafkaConfig.TOPIC_LIVE_SHIPMENTS,
            KafkaConfig.TOPIC_LIVE_DISPATCHES,
        ]

    consume(topics_to_consume, args.max_messages)
