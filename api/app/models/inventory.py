"""
SQLAlchemy ORM models — mapped to existing PostgreSQL tables.
Tables were created by SQL migrations; models are read-only here.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from sqlalchemy import String, Integer, Numeric, Boolean, Date, DateTime, Text, BigInteger
from sqlalchemy.orm import Mapped, mapped_column
from api.app.db import Base


class Product(Base):
    __tablename__ = "products"
    product_id:       Mapped[str]            = mapped_column(String(10), primary_key=True)
    product_name:     Mapped[str]            = mapped_column(String(100))
    category:         Mapped[str]            = mapped_column(String(50))
    sku:              Mapped[str]            = mapped_column(String(30))
    unit_cost:        Mapped[Optional[Decimal]] = mapped_column(Numeric(10,2))
    unit_price:       Mapped[Decimal]        = mapped_column(Numeric(10,2))
    reorder_point:    Mapped[int]            = mapped_column(Integer)
    reorder_quantity: Mapped[int]            = mapped_column(Integer)
    weight_kg:        Mapped[Optional[Decimal]] = mapped_column(Numeric(8,2))
    supplier_id:      Mapped[str]            = mapped_column(String(10))
    is_active:        Mapped[bool]           = mapped_column(Boolean)


class Store(Base):
    __tablename__ = "stores"
    store_id:                  Mapped[str]           = mapped_column(String(10), primary_key=True)
    store_name:                Mapped[str]           = mapped_column(String(100))
    region:                    Mapped[Optional[str]] = mapped_column(String(30))
    city:                      Mapped[str]           = mapped_column(String(60))
    warehouse_capacity_units:  Mapped[Optional[int]] = mapped_column(Integer)
    is_active:                 Mapped[bool]          = mapped_column(Boolean)


class Supplier(Base):
    __tablename__ = "suppliers"
    supplier_id:       Mapped[str]            = mapped_column(String(10), primary_key=True)
    supplier_name:     Mapped[str]            = mapped_column(String(100))
    contact_email:     Mapped[Optional[str]]  = mapped_column(String(150))
    country:           Mapped[Optional[str]]  = mapped_column(String(60))
    lead_time_days:    Mapped[Optional[int]]  = mapped_column(Integer)
    reliability_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(4,2))
    is_active:         Mapped[bool]           = mapped_column(Boolean)


class Alert(Base):
    __tablename__ = "alerts"
    alert_id:        Mapped[int]            = mapped_column(BigInteger, primary_key=True)
    alert_type:      Mapped[str]            = mapped_column(String(30))
    severity:        Mapped[str]            = mapped_column(String(10))
    source:          Mapped[str]            = mapped_column(String(10))
    product_id:      Mapped[Optional[str]]  = mapped_column(String(10))
    store_id:        Mapped[Optional[str]]  = mapped_column(String(10))
    supplier_id:     Mapped[Optional[str]]  = mapped_column(String(10))
    message:         Mapped[str]            = mapped_column(Text)
    metric_value:    Mapped[Optional[Decimal]] = mapped_column(Numeric(12,2))
    threshold_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(12,2))
    is_resolved:     Mapped[bool]           = mapped_column(Boolean, default=False)
    resolved_at:     Mapped[Optional[datetime]] = mapped_column(DateTime)
    created_at:      Mapped[datetime]       = mapped_column(DateTime)


class DailyStockSnapshot(Base):
    __tablename__ = "batch_daily_stock_snapshot"
    snapshot_id:      Mapped[int]     = mapped_column(Integer, primary_key=True)
    product_id:       Mapped[str]     = mapped_column(String(10))
    store_id:         Mapped[str]     = mapped_column(String(10))
    snapshot_date:    Mapped[date]    = mapped_column(Date)
    opening_stock:    Mapped[int]     = mapped_column(Integer)
    total_inbound:    Mapped[int]     = mapped_column(Integer)
    total_outbound:   Mapped[int]     = mapped_column(Integer)
    total_adjustment: Mapped[int]     = mapped_column(Integer)
    closing_stock:    Mapped[int]     = mapped_column(Integer)
    reorder_point:    Mapped[int]     = mapped_column(Integer)
    is_below_reorder: Mapped[bool]    = mapped_column(Boolean)
    computed_at:      Mapped[datetime]= mapped_column(DateTime)


class OverstockReport(Base):
    __tablename__ = "batch_overstock_report"
    overstock_id:           Mapped[int]            = mapped_column(Integer, primary_key=True)
    product_id:             Mapped[str]            = mapped_column(String(10))
    store_id:               Mapped[str]            = mapped_column(String(10))
    report_date:            Mapped[date]           = mapped_column(Date)
    current_stock_units:    Mapped[int]            = mapped_column(Integer)
    avg_daily_demand_units: Mapped[Decimal]        = mapped_column(Numeric(8,2))
    days_of_supply:         Mapped[Optional[Decimal]] = mapped_column(Numeric(8,1))
    reorder_point_units:    Mapped[int]            = mapped_column(Integer)
    excess_units:           Mapped[Optional[int]]  = mapped_column(Integer)
    excess_value:           Mapped[Optional[Decimal]] = mapped_column(Numeric(14,2))
    overstock_severity:     Mapped[Optional[str]]  = mapped_column(String(10))
    computed_at:            Mapped[datetime]       = mapped_column(DateTime)


class SupplierPerformance(Base):
    __tablename__ = "batch_supplier_performance"
    perf_id:              Mapped[int]            = mapped_column(Integer, primary_key=True)
    supplier_id:          Mapped[str]            = mapped_column(String(10))
    report_period_start:  Mapped[date]           = mapped_column(Date)
    report_period_end:    Mapped[date]           = mapped_column(Date)
    total_orders:         Mapped[int]            = mapped_column(Integer)
    on_time_deliveries:   Mapped[int]            = mapped_column(Integer)
    late_deliveries:      Mapped[int]            = mapped_column(Integer)
    cancelled_orders:     Mapped[int]            = mapped_column(Integer)
    on_time_rate:         Mapped[Optional[Decimal]] = mapped_column(Numeric(5,2))
    avg_lead_time_days:   Mapped[Optional[Decimal]] = mapped_column(Numeric(6,2))
    total_freight_cost:   Mapped[Optional[Decimal]] = mapped_column(Numeric(14,2))
    fill_rate:            Mapped[Optional[Decimal]] = mapped_column(Numeric(5,2))
    computed_at:          Mapped[datetime]       = mapped_column(DateTime)


class InventoryTurnover(Base):
    __tablename__ = "batch_inventory_turnover"
    turnover_id:        Mapped[int]            = mapped_column(Integer, primary_key=True)
    product_id:         Mapped[str]            = mapped_column(String(10))
    store_id:           Mapped[str]            = mapped_column(String(10))
    category:           Mapped[str]            = mapped_column(String(50))
    report_month:       Mapped[date]           = mapped_column(Date)
    total_units_sold:   Mapped[int]            = mapped_column(Integer)
    total_revenue:      Mapped[Decimal]        = mapped_column(Numeric(14,2))
    avg_inventory:      Mapped[Optional[Decimal]] = mapped_column(Numeric(10,2))
    turnover_ratio:     Mapped[Optional[Decimal]] = mapped_column(Numeric(8,4))
    days_inventory_held:Mapped[Optional[Decimal]] = mapped_column(Numeric(8,1))
    computed_at:        Mapped[datetime]       = mapped_column(DateTime)


class StockMovement(Base):
    __tablename__ = "stock_movements"
    movement_id:   Mapped[str]            = mapped_column(String(10), primary_key=True)
    product_id:    Mapped[str]            = mapped_column(String(10))
    store_id:      Mapped[Optional[str]]  = mapped_column(String(10))
    movement_type: Mapped[str]            = mapped_column(String(20))
    quantity:      Mapped[int]            = mapped_column(Integer)
    movement_date: Mapped[datetime]       = mapped_column(DateTime)
    reference_id:  Mapped[Optional[str]]  = mapped_column(String(20))
    notes:         Mapped[Optional[str]]  = mapped_column(Text)
