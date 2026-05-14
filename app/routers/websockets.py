"""
WebSocket Endpoints
===================
Three rooms matching the frontend pages:

  /ws/admin          → AdminPage  subscribes here
  /ws/agent/{id}     → AgentPage  subscribes here (each agent in their own room)
  /ws/display        → Public display screen
  /ws/ticket/{id}    → Citizen waiting page (notification when called)

The frontend connects on mount and disconnects on unmount.
On connection, we immediately push the current queue state so the client
doesn't have to wait for the next event.
"""

import json
from uuid import UUID
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.core.websocket_manager import manager
from app.services import ticket_service

router = APIRouter(tags=["WebSocket"])


@router.websocket("/ws/admin")
async def ws_admin(websocket: WebSocket, db: AsyncSession = Depends(get_db)):
    await manager.connect(websocket, "admin")
    try:
        stats = await ticket_service.get_stats(db)
        await websocket.send_json({"type": "stats_updated", **stats.model_dump()})

        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            if msg.get("type") == "ping":
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        manager.disconnect(websocket, "admin")


@router.websocket("/ws/agent/{agent_id}")
async def ws_agent(websocket: WebSocket, agent_id: UUID, db: AsyncSession = Depends(get_db)):
    room = f"agent:{agent_id}"
    await manager.connect(websocket, room)
    try:
        queue = await ticket_service.get_queue_for_agent(db, agent_id)
        await websocket.send_json({
            "type": "queue_snapshot",
            "tickets": [t.model_dump(mode="json") for t in queue],
        })

        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            if msg.get("type") == "ping":
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        manager.disconnect(websocket, room)


@router.websocket("/ws/display")
async def ws_display(websocket: WebSocket):
    """Public display screen — shows the currently called ticket."""
    await manager.connect(websocket, "display")
    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            if msg.get("type") == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        manager.disconnect(websocket, "display")


@router.websocket("/ws/ticket/{ticket_id}")
async def ws_ticket(websocket: WebSocket, ticket_id: UUID):
    """Citizen waiting page — notifies when their ticket is called."""
    room = f"ticket:{ticket_id}"
    await manager.connect(websocket, room)
    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            if msg.get("type") == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        manager.disconnect(websocket, room)