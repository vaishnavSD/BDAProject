from typing import Optional, List
from datetime import date
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from api.app.db import get_db
from api.app.schemas.responses import StockSnapshotOut, StoreOut, DashboardSummaryOut
from api.app.services.inventory_service import (
    get_stock_snapshot, get_stores, get_store,
    get_dashboard_summary, get_latest_snapshot_date,
)

router = APIRouter(prefix="/inventory", tags=["Inventory"])

@router.get("/summary", response_model=DashboardSummaryOut)
async def dashboard_summary(db: AsyncSession = Depends(get_db)):
    return await get_dashboard_summary(db)

@router.get("/stores", response_model=List[StoreOut])
async def list_stores(db: AsyncSession = Depends(get_db)):
    return await get_stores(db)

@router.get("/stores/{store_id}", response_model=StoreOut)
async def get_store_detail(store_id: str, db: AsyncSession = Depends(get_db)):
    from fastapi import HTTPException
    s = await get_store(db, store_id)
    if not s:
        raise HTTPException(status_code=404, detail=f"Store {store_id} not found")
    return s

@router.get("/snapshot", response_model=List[StockSnapshotOut])
async def stock_snapshot(
    snapshot_date: Optional[date] = Query(None),
    store_id: Optional[str] = Query(None),
    below_reorder_only: bool = Query(False),
    limit: int = Query(500, le=1000),
    db: AsyncSession = Depends(get_db),
):
    return await get_stock_snapshot(db, snapshot_date, store_id, below_reorder_only, limit)

@router.get("/snapshot/latest-date")
async def latest_snapshot_date(db: AsyncSession = Depends(get_db)):
    d = await get_latest_snapshot_date(db)
    return {"latest_snapshot_date": d}
