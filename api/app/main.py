"""
main.py — FastAPI application entry point.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.app.routers import products, inventory, alerts, reports

app = FastAPI(
    title="Smart Inventory Management API",
    description="REST + WebSocket API for the Smart Inventory Management System",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Allow React dev server (port 3000) and production build
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(products.router)
app.include_router(inventory.router)
app.include_router(alerts.router)
app.include_router(reports.router)


@app.get("/", tags=["Health"])
async def root():
    return {"status": "ok", "service": "Smart Inventory Management API", "version": "1.0.0"}


@app.get("/health", tags=["Health"])
async def health():
    return {"status": "healthy"}
