"""
Schema validation test against real CSV data.
Runs without Kafka — purely validates Pydantic schemas against every row.
"""

import csv
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from schemas.events import LiveOrderEvent, LiveShipmentEvent, LiveDispatchEvent
from config.settings import DataPaths
from pydantic import ValidationError

def test_csv(label, csv_path, schema_class):
    passed = failed = 0
    errors = []
    with open(csv_path, "r", encoding="utf-8") as f:
        for row_num, row in enumerate(csv.DictReader(f), start=2):
            clean = {k: v.strip() for k, v in row.items()}
            try:
                schema_class(**clean)
                passed += 1
            except (ValidationError, ValueError) as e:
                failed += 1
                errors.append(f"  Row {row_num}: {str(e)[:120]}")

    print(f"\n{label}")
    print(f"  Passed : {passed}")
    print(f"  Failed : {failed}")
    for e in errors[:5]:
        print(e)
    if len(errors) > 5:
        print(f"  ... and {len(errors)-5} more")

test_csv("live_orders.csv    → LiveOrderEvent",    DataPaths.LIVE_ORDERS,     LiveOrderEvent)
test_csv("live_shipments.csv → LiveShipmentEvent", DataPaths.LIVE_SHIPMENTS,  LiveShipmentEvent)
test_csv("live_dispatches.csv→ LiveDispatchEvent", DataPaths.LIVE_DISPATCHES, LiveDispatchEvent)

print("\nSchema validation complete.")
