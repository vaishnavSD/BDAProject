"""
Pydantic response schemas — what the API returns to clients.
Separate from ORM models so the API contract is decoupled from DB shape.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List
from pydantic import BaseModel, ConfigDict


class ProductOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    product_id: str
    product_name: str
    category: str
    sku: str
    unit_cost: Optional[Decimal]
    unit_price: Decimal
    reorder_point: int
    reorder_quantity: int
    supplier_id: str
    is_active: bool


class StoreOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    store_id: str
    store_name: str
    region: Optional[str]
    city: str
    warehouse_capacity_units: Optional[int]
    is_active: bool


class SupplierOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    supplier_id: str
    supplier_name: str
    country: Optional[str]
    lead_time_days: Optional[int]
    reliability_score: Optional[Decimal]
    is_active: bool


class AlertOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    alert_id: int
    alert_type: str
    severity: str
    source: str
    product_id: Optional[str]
    store_id: Optional[str]
    supplier_id: Optional[str]
    message: str
    metric_value: Optional[Decimal]
    threshold_value: Optional[Decimal]
    is_resolved: bool
    created_at: datetime


class StockSnapshotOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    snapshot_id: int
    product_id: str
    store_id: str
    snapshot_date: date
    opening_stock: int
    total_inbound: int
    total_outbound: int
    total_adjustment: int
    closing_stock: int
    reorder_point: int
    is_below_reorder: bool
    computed_at: datetime


class OverstockOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    overstock_id: int
    product_id: str
    store_id: str
    report_date: date
    current_stock_units: int
    avg_daily_demand_units: Decimal
    days_of_supply: Optional[Decimal]
    reorder_point_units: int
    excess_units: Optional[int]
    excess_value: Optional[Decimal]
    overstock_severity: Optional[str]


class SupplierPerfOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    perf_id: int
    supplier_id: str
    report_period_start: date
    report_period_end: date
    total_orders: int
    on_time_deliveries: int
    late_deliveries: int
    cancelled_orders: int
    on_time_rate: Optional[Decimal]
    avg_lead_time_days: Optional[Decimal]
    total_freight_cost: Optional[Decimal]
    fill_rate: Optional[Decimal]


class TurnoverOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    turnover_id: int
    product_id: str
    store_id: str
    category: str
    report_month: date
    total_units_sold: int
    total_revenue: Decimal
    avg_inventory: Optional[Decimal]
    turnover_ratio: Optional[Decimal]
    days_inventory_held: Optional[Decimal]


class InventoryLiveOut(BaseModel):
    """Live inventory summary per product+store from stream tables."""
    product_id: str
    store_id: str
    total_ordered_live: int
    total_dispatched_live: int
    total_in_transit_live: int
    latest_event_ts: Optional[datetime]


class DashboardSummaryOut(BaseModel):
    """Single-call summary for the dashboard home page."""
    total_products: int
    total_stores: int
    total_suppliers: int
    unresolved_alerts: int
    critical_alerts: int
    stockout_risk_products: int
    overstock_products: int
    latest_snapshot_date: Optional[date]


class PaginatedAlerts(BaseModel):
    total: int
    page: int
    page_size: int
    items: List[AlertOut]
