"""
inventory_service.py
Business logic layer — all DB queries live here, not in routers.
Routers call these functions and return the results directly.
"""

from datetime import date
from typing import Optional, List

from sqlalchemy import select, func, text, desc, and_
from sqlalchemy.ext.asyncio import AsyncSession

from api.app.models.inventory import (
    Product, Store, Supplier, Alert,
    DailyStockSnapshot, OverstockReport,
    SupplierPerformance, InventoryTurnover,
    StockMovement,
)


# ── Products ──────────────────────────────────────────────────────────────────

async def get_products(db: AsyncSession, skip: int = 0, limit: int = 100, category: Optional[str] = None):
    q = select(Product).where(Product.is_active == True)
    if category:
        q = q.where(Product.category == category)
    q = q.offset(skip).limit(limit).order_by(Product.product_id)
    result = await db.execute(q)
    return result.scalars().all()


async def get_product(db: AsyncSession, product_id: str):
    result = await db.execute(select(Product).where(Product.product_id == product_id))
    return result.scalar_one_or_none()


async def get_categories(db: AsyncSession):
    result = await db.execute(select(Product.category).distinct().order_by(Product.category))
    return [r[0] for r in result.fetchall()]


# ── Stores ────────────────────────────────────────────────────────────────────

async def get_stores(db: AsyncSession):
    result = await db.execute(select(Store).where(Store.is_active == True).order_by(Store.store_id))
    return result.scalars().all()


async def get_store(db: AsyncSession, store_id: str):
    result = await db.execute(select(Store).where(Store.store_id == store_id))
    return result.scalar_one_or_none()


# ── Suppliers ─────────────────────────────────────────────────────────────────

async def get_suppliers(db: AsyncSession):
    result = await db.execute(select(Supplier).order_by(Supplier.supplier_id))
    return result.scalars().all()


# ── Alerts ────────────────────────────────────────────────────────────────────

async def get_alerts(
    db: AsyncSession,
    page: int = 1,
    page_size: int = 20,
    severity: Optional[str] = None,
    unresolved_only: bool = True,
):
    q = select(Alert)
    if unresolved_only:
        q = q.where(Alert.is_resolved == False)
    if severity:
        q = q.where(Alert.severity == severity)
    q = q.order_by(desc(Alert.created_at))

    count_q = select(func.count()).select_from(q.subquery())
    total = (await db.execute(count_q)).scalar_one()

    q = q.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(q)
    items = result.scalars().all()
    return total, items


async def get_latest_alerts(db: AsyncSession, limit: int = 10):
    """Used by WebSocket polling — newest unresolved alerts."""
    result = await db.execute(
        select(Alert)
        .where(Alert.is_resolved == False)
        .order_by(desc(Alert.created_at))
        .limit(limit)
    )
    return result.scalars().all()


async def resolve_alert(db: AsyncSession, alert_id: int):
    from datetime import datetime, timezone
    result = await db.execute(select(Alert).where(Alert.alert_id == alert_id))
    alert = result.scalar_one_or_none()
    if alert:
        alert.is_resolved = True
        alert.resolved_at = datetime.now(timezone.utc)
        await db.commit()
    return alert


# ── Daily Stock Snapshot ──────────────────────────────────────────────────────

async def get_stock_snapshot(
    db: AsyncSession,
    snapshot_date: Optional[date] = None,
    store_id: Optional[str] = None,
    below_reorder_only: bool = False,
    limit: int = 500,
):
    q = select(DailyStockSnapshot)
    if snapshot_date:
        q = q.where(DailyStockSnapshot.snapshot_date == snapshot_date)
    if store_id:
        q = q.where(DailyStockSnapshot.store_id == store_id)
    if below_reorder_only:
        q = q.where(DailyStockSnapshot.is_below_reorder == True)
    q = q.order_by(desc(DailyStockSnapshot.snapshot_date), DailyStockSnapshot.product_id).limit(limit)
    result = await db.execute(q)
    return result.scalars().all()


async def get_latest_snapshot_date(db: AsyncSession) -> Optional[date]:
    result = await db.execute(select(func.max(DailyStockSnapshot.snapshot_date)))
    return result.scalar_one_or_none()


# ── Overstock Report ──────────────────────────────────────────────────────────

async def get_overstock_report(
    db: AsyncSession,
    report_date: Optional[date] = None,
    severity: Optional[str] = None,
    limit: int = 200,
):
    q = select(OverstockReport)
    if report_date:
        q = q.where(OverstockReport.report_date == report_date)
    if severity:
        q = q.where(OverstockReport.overstock_severity == severity)
    q = q.order_by(desc(OverstockReport.report_date), desc(OverstockReport.excess_value)).limit(limit)
    result = await db.execute(q)
    return result.scalars().all()


# ── Supplier Performance ──────────────────────────────────────────────────────

async def get_supplier_performance(
    db: AsyncSession,
    supplier_id: Optional[str] = None,
    limit: int = 100,
):
    q = select(SupplierPerformance)
    if supplier_id:
        q = q.where(SupplierPerformance.supplier_id == supplier_id)
    q = q.order_by(desc(SupplierPerformance.report_period_start)).limit(limit)
    result = await db.execute(q)
    return result.scalars().all()


# ── Inventory Turnover ────────────────────────────────────────────────────────

async def get_turnover_report(
    db: AsyncSession,
    report_month: Optional[str] = None,
    category: Optional[str] = None,
    limit: int = 200,
):
    q = select(InventoryTurnover)
    if report_month:
        month_date = date.fromisoformat(f"{report_month}-01")
        q = q.where(InventoryTurnover.report_month == month_date)
    if category:
        q = q.where(InventoryTurnover.category == category)
    q = q.order_by(desc(InventoryTurnover.report_month), InventoryTurnover.turnover_ratio).limit(limit)
    result = await db.execute(q)
    return result.scalars().all()


# ── Dashboard Summary ─────────────────────────────────────────────────────────

async def get_dashboard_summary(db: AsyncSession) -> dict:
    total_products   = (await db.execute(select(func.count()).where(Product.is_active == True).select_from(Product))).scalar_one()
    total_stores     = (await db.execute(select(func.count()).where(Store.is_active == True).select_from(Store))).scalar_one()
    total_suppliers  = (await db.execute(select(func.count()).select_from(Supplier))).scalar_one()
    unresolved       = (await db.execute(select(func.count()).where(Alert.is_resolved == False).select_from(Alert))).scalar_one()
    critical         = (await db.execute(select(func.count()).where(and_(Alert.is_resolved == False, Alert.severity == "CRITICAL")).select_from(Alert))).scalar_one()
    latest_snap_date = await get_latest_snapshot_date(db)

    stockout = (await db.execute(
        select(func.count()).where(
            and_(DailyStockSnapshot.snapshot_date == latest_snap_date,
                 DailyStockSnapshot.is_below_reorder == True)
        ).select_from(DailyStockSnapshot)
    )).scalar_one() if latest_snap_date else 0

    overstock = (await db.execute(
        select(func.count()).where(OverstockReport.report_date == latest_snap_date).select_from(OverstockReport)
    )).scalar_one() if latest_snap_date else 0

    return {
        "total_products": total_products,
        "total_stores": total_stores,
        "total_suppliers": total_suppliers,
        "unresolved_alerts": unresolved,
        "critical_alerts": critical,
        "stockout_risk_products": stockout,
        "overstock_products": overstock,
        "latest_snapshot_date": latest_snap_date,
    }
