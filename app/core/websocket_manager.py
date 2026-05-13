
#app\core\websocket_manager.py
"""
WebSocket Manager
=================
Handles real-time communication using rooms.
"""

import asyncio
import json
import logging
from typing import Dict, Set
from fastapi import WebSocket

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# Connection Manager
# ─────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self._rooms: Dict[str, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, room: str):
        await websocket.accept()
        self._rooms.setdefault(room, set()).add(websocket)
        logger.info(f"WS connected → {room}")

    def disconnect(self, websocket: WebSocket, room: str):
        if room in self._rooms:
            self._rooms[room].discard(websocket)
            if not self._rooms[room]:
                del self._rooms[room]

    async def broadcast_to_room(self, room: str, payload: dict):
        connections = list(self._rooms.get(room, set()))
        for ws in connections:
            try:
                await ws.send_json(payload)
            except:
                self.disconnect(ws, room)

    async def dispatch_event(self, raw):
        try:
            event = json.loads(raw)
        except:
            return

        event_type = event.get("type")
        agent_id = event.get("agent_id")

        # Admin always receives
        await self.broadcast_to_room("admin", event)

        # Agent room
        if agent_id:
            await self.broadcast_to_room(f"agent:{agent_id}", event)

        # Public display
        if event_type == "ticket_called":
            await self.broadcast_to_room("display", event)


# ✅ IMPORTANT: THIS WAS MISSING
manager = ConnectionManager()


# ─────────────────────────────────────────
# Redis Subscriber
# ─────────────────────────────────────────
async def redis_subscriber_loop():
    from app.core.redis import redis_client

    channel = "cnas:events"

    while True:
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(channel)

            logger.info(f"Redis subscriber listening on channel: {channel}")

            async for message in pubsub.listen():
                if message["type"] == "message":
                    await manager.dispatch_event(message["data"])

        except Exception as e:
            logger.error(f"Redis subscriber error: {e} → retry in 3s")
            await asyncio.sleep(3)


            