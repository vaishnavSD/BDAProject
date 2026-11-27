"""
API integration tests — run with pytest from project root.
Requires a running PostgreSQL instance.
"""

import pytest
from httpx import AsyncClient, ASGITransport
from api.app.main import app


@pytest.mark.asyncio
async def test_health():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "healthy"


@pytest.mark.asyncio
async def test_root():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/")
    assert r.status_code == 200
    assert "version" in r.json()


@pytest.mark.asyncio
async def test_products_endpoint():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/products/?limit=5")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


@pytest.mark.asyncio
async def test_stores_endpoint():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/inventory/stores")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_alerts_endpoint():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/alerts/?page=1&page_size=5")
    assert r.status_code == 200
    body = r.json()
    assert "total" in body
    assert "items" in body


@pytest.mark.asyncio
async def test_summary_endpoint():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/inventory/summary")
    assert r.status_code == 200
    body = r.json()
    assert "total_products" in body
    assert "unresolved_alerts" in body
