from typing import Optional, List
from datetime import date
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from api.app.db import get_db
from api.app.schemas.responses import OverstockOut, SupplierPerfOut, TurnoverOut, SupplierOut
from api.app.services.inventory_service import (
    get_overstock_report, get_supplier_performance,
    get_turnover_report, get_suppliers,
)

router = APIRouter(prefix="/reports", tags=["Reports"])


@router.get("/overstock", response_model=List[OverstockOut])
async def overstock_report(
    report_date: Optional[date] = Query(None),
    severity: Optional[str] = Query(None),
    limit: int = Query(200, le=500),
    db: AsyncSession = Depends(get_db),
):
    return await get_overstock_report(db, report_date, severity, limit)


@router.get("/supplier-performance", response_model=List[SupplierPerfOut])
async def supplier_performance(
    supplier_id: Optional[str] = Query(None),
    limit: int = Query(100, le=500),
    db: AsyncSession = Depends(get_db),
):
    return await get_supplier_performance(db, supplier_id, limit)


@router.get("/inventory-turnover", response_model=List[TurnoverOut])
async def inventory_turnover(
    month: Optional[str] = Query(None, description="YYYY-MM format"),
    category: Optional[str] = Query(None),
    limit: int = Query(200, le=500),
    db: AsyncSession = Depends(get_db),
):
    return await get_turnover_report(db, month, category, limit)


@router.get("/suppliers", response_model=List[SupplierOut])
async def supplier_list(db: AsyncSession = Depends(get_db)):
    return await get_suppliers(db)
