"""
WebSocket Manager
=================
Manages three WebSocket rooms matching the frontend pages:

  Room "admin"          → AdminPage  (live stats, agent list, service counts)
  Room "agent:{id}"     → AgentPage  (personal queue, ticket state)
  Room "display"        → Public display screen (current ticket called)

Flow:
  1. Client connects → joins a room
  2. A ticket action happens (call_next, skip, pause…)
  3. ticket_service publishes an event to Redis pub/sub channel "cnas:events"
  4. The Redis subscriber (started at boot) receives the event
  5. Manager broadcasts to every relevant room
"""

import asyncio
import json
import logging
from typing import Dict, Set
from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self):
        # room_id → set of active WebSocket connections
        self._rooms: Dict[str, Set[WebSocket]] = {}

    # ── Connection lifecycle ──────────────────────────────────────────────────

    async def connect(self, websocket: WebSocket, room: str):
        await websocket.accept()
        self._rooms.setdefault(room, set()).add(websocket)
        logger.info(f"WS connected: room={room}, total={len(self._rooms[room])}")

    def disconnect(self, websocket: WebSocket, room: str):
        if room in self._rooms:
            self._rooms[room].discard(websocket)
            if not self._rooms[room]:
                del self._rooms[room]
        logger.info(f"WS disconnected: room={room}")

    # ── Broadcast helpers ─────────────────────────────────────────────────────

    async def broadcast_to_room(self, room: str, payload: dict):
        """Send a JSON payload to every client in a room."""
        connections = list(self._rooms.get(room, set()))
        dead: list[WebSocket] = []
        for ws in connections:
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._rooms.get(room, set()).discard(ws)

    async def broadcast_to_all(self, payload: dict):
        """Broadcast to every connected client across all rooms."""
        for room in list(self._rooms.keys()):
            await self.broadcast_to_room(room, payload)

    # ── Event dispatcher (called by Redis subscriber) ─────────────────────────

    async def dispatch_event(self, raw: str):
        """
        Parse a Redis pub/sub message and route it to the right rooms.

        Event types published by ticket_service:
          ticket_called    → admin + agent:{agent_id} + display
          ticket_skipped   → admin + agent:{agent_id}
          ticket_done      → admin + agent:{agent_id}
          queue_updated    → admin + agent:{agent_id}
          agent_paused     → admin + agent:{agent_id}
          agent_assigned   → admin
          stats_updated    → admin
        """
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            return

        event_type = event.get("type")
        agent_id   = event.get("agent_id")

        # Always push to admin room for the live dashboard
        await self.broadcast_to_room("admin", event)

        # Push to the specific agent's room
        if agent_id:
            await self.broadcast_to_room(f"agent:{agent_id}", event)

        # Push to the public display screen for ticket-call events
        if event_type in ("ticket_called",):
            await self.broadcast_to_room("display", event)


# Singleton shared across the app
manager = ConnectionManager()


# ── Redis subscriber loop ─────────────────────────────────────────────────────

async def redis_subscriber_loop():
    """
    Long-running task started at app startup.
    Subscribes to the Redis channel and feeds messages into the WS manager.
    """
    from app.core.redis import redis_client
    import asyncio

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
            logger.error(f"Redis subscriber error: {e}. Reconnecting in 3s…")
            await asyncio.sleep(3)
