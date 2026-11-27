"""
alerts.py — Alert REST endpoints and WebSocket live feed.
"""

import asyncio
import json
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.app.db import get_db, AsyncSessionLocal
from api.app.models.inventory import Alert
from api.app.schemas.responses import PaginatedAlerts, AlertOut
from api.app.services.inventory_service import get_alerts, get_latest_alerts, resolve_alert

router = APIRouter(prefix="/alerts", tags=["Alerts"])


@router.get("/", response_model=PaginatedAlerts)
async def list_alerts(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, le=100),
    severity: Optional[str] = Query(None),
    unresolved_only: bool = Query(True),
    db: AsyncSession = Depends(get_db),
):
    total, items = await get_alerts(db, page, page_size, severity, unresolved_only)
    return PaginatedAlerts(total=total, page=page, page_size=page_size, items=items)


@router.patch("/{alert_id}/resolve", response_model=AlertOut)
async def resolve_alert_endpoint(alert_id: int, db: AsyncSession = Depends(get_db)):
    alert = await resolve_alert(db, alert_id)
    if not alert:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")
    return alert


@router.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    """
    WebSocket endpoint — pushes new unresolved alerts to the React dashboard.

    Protocol:
      Server → Client: JSON array of alert objects (new since last poll)
      Client → Server: 'ping' (keepalive)
    Polls the alerts table every 5 seconds.
    """
    await websocket.accept()
    last_seen_id: int = 0

    try:
        while True:
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(Alert)
                    .where(Alert.is_resolved == False)
                    .where(Alert.alert_id > last_seen_id)
                    .order_by(Alert.alert_id)
                    .limit(50)
                )
                new_alerts = result.scalars().all()

            if new_alerts:
                last_seen_id = max(a.alert_id for a in new_alerts)
                payload = [
                    {
                        "alert_id":        a.alert_id,
                        "alert_type":      a.alert_type,
                        "severity":        a.severity,
                        "source":          a.source,
                        "product_id":      a.product_id,
                        "store_id":        a.store_id,
                        "supplier_id":     a.supplier_id,
                        "message":         a.message,
                        "metric_value":    str(a.metric_value)    if a.metric_value    else None,
                        "threshold_value": str(a.threshold_value) if a.threshold_value else None,
                        "is_resolved":     a.is_resolved,
                        "created_at":      a.created_at.isoformat(),
                    }
                    for a in new_alerts
                ]
                await websocket.send_text(json.dumps(payload))

            # Wait 5 s, but also accept client messages (ping/keepalive)
            try:
                msg = await asyncio.wait_for(websocket.receive_text(), timeout=5.0)
                if msg == "ping":
                    await websocket.send_text(json.dumps({"pong": True}))
            except asyncio.TimeoutError:
                pass  # expected — no client message in this window

    except WebSocketDisconnect:
        pass
