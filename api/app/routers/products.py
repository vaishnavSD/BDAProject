from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from api.app.db import get_db
from api.app.schemas.responses import ProductOut
from api.app.services.inventory_service import get_products, get_product, get_categories

router = APIRouter(prefix="/products", tags=["Products"])

@router.get("/", response_model=List[ProductOut])
async def list_products(
    skip: int = 0,
    limit: int = 100,
    category: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    return await get_products(db, skip=skip, limit=limit, category=category)

@router.get("/categories", response_model=List[str])
async def list_categories(db: AsyncSession = Depends(get_db)):
    return await get_categories(db)

@router.get("/{product_id}", response_model=ProductOut)
async def get_product_detail(product_id: str, db: AsyncSession = Depends(get_db)):
    p = await get_product(db, product_id)
    if not p:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found")
    return p
