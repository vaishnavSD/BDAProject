"""
Pydantic v2 schemas for all Kafka event types.

Each schema:
  - Maps directly to a row in the corresponding streaming CSV.
  - Validates field types and business rules before the row is published.
  - Invalid rows are routed to the Dead Letter Queue (DLQ) topic,
    not silently dropped.
"""

from __future__ import annotations
from datetime import datetime
from decimal import Decimal
from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator, model_validator


# ─────────────────────────────────────────────────────────────────────────────
# LiveOrderEvent
# Source: data/raw/streaming/live_orders.csv
# Kafka topic: inventory-live-orders
# ─────────────────────────────────────────────────────────────────────────────
class LiveOrderEvent(BaseModel):
    live_order_id:    str            = Field(..., min_length=1, max_length=10)
    product_id:       str            = Field(..., pattern=r"^PRD\d{4}$")
    store_id:         str            = Field(..., pattern=r"^STR\d{3}$")
    supplier_id:      str            = Field(..., pattern=r"^SUP\d{3}$")
    event_timestamp:  datetime
    quantity_ordered: int            = Field(..., gt=0)
    unit_price:       Decimal        = Field(..., gt=0)
    total_amount:     Decimal        = Field(..., gt=0)
    order_status:     Literal["PENDING", "CONFIRMED"]
    event_type:       Literal["ORDER_PLACED"]
    source_system:    Literal["WEB", "MOBILE", "B2B_PORTAL", "EDI"]

    @model_validator(mode="after")
    def validate_total_amount(self) -> "LiveOrderEvent":
        expected = round(self.quantity_ordered * float(self.unit_price), 2)
        actual   = float(self.total_amount)
        # Allow ±1% tolerance for floating-point rounding in source system
        if abs(expected - actual) / max(expected, 1) > 0.01:
            raise ValueError(
                f"total_amount {actual} does not match "
                f"quantity × unit_price = {expected}"
            )
        return self

    def to_kafka_dict(self) -> dict:
        """Serialisable dict for JSON encoding."""
        d = self.model_dump()
        d["event_timestamp"] = self.event_timestamp.isoformat()
        d["unit_price"]      = str(self.unit_price)
        d["total_amount"]    = str(self.total_amount)
        return d


# ─────────────────────────────────────────────────────────────────────────────
# LiveShipmentEvent
# Source: data/raw/streaming/live_shipments.csv
# Kafka topic: inventory-live-shipments
# ─────────────────────────────────────────────────────────────────────────────
class LiveShipmentEvent(BaseModel):
    live_shipment_id:   str      = Field(..., min_length=1, max_length=10)
    shipment_id:        str      = Field(..., min_length=1, max_length=10)
    product_id:         str      = Field(..., pattern=r"^PRD\d{4}$")
    store_id:           str      = Field(..., pattern=r"^STR\d{3}$")
    event_timestamp:    datetime
    shipment_status:    Literal[
                            "DISPATCHED", "IN_TRANSIT",
                            "OUT_FOR_DELIVERY", "DELIVERED", "DELAYED"
                        ]
    current_location:   Optional[str] = None
    estimated_arrival:  Optional[str] = None     # DATE string "YYYY-MM-DD"
    quantity_in_transit: int     = Field(..., gt=0)
    event_type:         Literal["SHIPMENT_UPDATE"]
    carrier:            Optional[str] = None

    @field_validator("estimated_arrival", mode="before")
    @classmethod
    def parse_arrival_date(cls, v):
        if v == "" or v is None:
            return None
        # Validate format only; store as string
        datetime.strptime(v, "%Y-%m-%d")
        return v

    def to_kafka_dict(self) -> dict:
        d = self.model_dump()
        d["event_timestamp"] = self.event_timestamp.isoformat()
        return d


# ─────────────────────────────────────────────────────────────────────────────
# LiveDispatchEvent
# Source: data/raw/streaming/live_dispatches.csv
# Kafka topic: inventory-live-dispatches
# ─────────────────────────────────────────────────────────────────────────────
class LiveDispatchEvent(BaseModel):
    dispatch_id:         str     = Field(..., min_length=1, max_length=10)
    product_id:          str     = Field(..., pattern=r"^PRD\d{4}$")
    from_store_id:       str     = Field(..., pattern=r"^STR\d{3}$")
    to_store_id:         str     = Field(..., pattern=r"^STR\d{3}$")
    event_timestamp:     datetime
    quantity_dispatched: int     = Field(..., gt=0)
    dispatch_reason:     Literal[
                             "RESTOCK", "TRANSFER",
                             "EMERGENCY_REPLENISH", "RETURN_TO_SUPPLIER"
                         ]
    dispatch_status:     Literal["INITIATED", "IN_TRANSIT", "COMPLETED", "CANCELLED"]
    event_type:          Literal["DISPATCH_EVENT"]
    priority:            Literal["NORMAL", "HIGH", "URGENT"]

    @field_validator("from_store_id", "to_store_id", mode="after")
    @classmethod
    def stores_must_differ(cls, v, info):
        # Cross-field check: from and to store cannot be the same
        # This runs on both fields; only enforce when both are present
        return v

    @model_validator(mode="after")
    def validate_different_stores(self) -> "LiveDispatchEvent":
        if self.from_store_id == self.to_store_id:
            raise ValueError(
                f"from_store_id and to_store_id cannot be the same: {self.from_store_id}"
            )
        return self

    def to_kafka_dict(self) -> dict:
        d = self.model_dump()
        d["event_timestamp"] = self.event_timestamp.isoformat()
        return d


# ─────────────────────────────────────────────────────────────────────────────
# DLQEvent  — wraps a failed row for the dead-letter queue topic
# ─────────────────────────────────────────────────────────────────────────────
class DLQEvent(BaseModel):
    source_topic:   str
    source_file:    str
    raw_row:        dict
    error_message:  str
    failed_at:      str   # ISO timestamp

    def to_kafka_dict(self) -> dict:
        return self.model_dump()
