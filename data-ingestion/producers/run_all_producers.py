"""
run_all_producers.py
─────────────────────────────────────────────────────────────────────────────
Launches all three Kafka producers in separate threads so they publish
concurrently — matching the real-world scenario where orders, shipments,
and dispatches arrive simultaneously.

Usage:
    python run_all_producers.py

Each producer thread runs independently. If one fails, the others continue.
Stats are logged per producer when each thread finishes.
"""

import sys
import os
import logging
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from data_ingestion.producers.live_orders_producer     import LiveOrdersProducer
from data_ingestion.producers.live_shipments_producer  import LiveShipmentsProducer
from data_ingestion.producers.live_dispatches_producer import LiveDispatchesProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ProducerRunner")


def run_producer(producer_instance, name: str):
    try:
        logger.info("Starting %s thread...", name)
        producer_instance.run()
    except Exception as e:
        logger.error("%s crashed: %s", name, e)


if __name__ == "__main__":
    producers = [
        (LiveOrdersProducer(),     "LiveOrdersProducer"),
        (LiveShipmentsProducer(),  "LiveShipmentsProducer"),
        (LiveDispatchesProducer(), "LiveDispatchesProducer"),
    ]

    threads = []
    for producer, name in producers:
        t = threading.Thread(target=run_producer, args=(producer, name), name=name, daemon=False)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    logger.info("All producers finished.")
