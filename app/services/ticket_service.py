#app\services\ticket_service.py
"""
Ticket Service
==============
Every action here:
  1. Updates PostgreSQL (permanent record)
  2. Updates Redis (fast queue state + waiting counts)
  3. Publishes an event to Redis pub/sub → WebSocket manager broadcasts to clients

This is the bridge between HTTP actions and real-time updates.
"""
import string
import json
from datetime import datetime, timezone
from uuid import UUID
from sqlalchemy import cast
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update
from fastapi import HTTPException
import redis.asyncio as aioredis
from app.models.models import Ticket, Agent, Service, TicketStatus
from app.schemas.schemas import TicketIssue, TicketOut
from app.config import settings
from app.models.models import Queue
import time
from sqlalchemy import func, select



def generate_prefix(service, mode: str, service_index: int = 0):

    # SINGLE MODE
    if mode == "single":

        if service.category.value == "prestation":
            return "A"

        return "B"

    # MULTI MODE
    alphabet = string.ascii_uppercase

    return alphabet[service_index]

def build_queue_key(category: str):
    return f"cnas:queue:{category}"

# ── Redis key helpers ─────────────────────────────────────────────────────────
 
def _stats_key() -> str:
    return "cnas:stats"


# ── Publish event to Redis pub/sub ────────────────────────────────────────────
async def _publish(redis: aioredis.Redis, event: dict):
    await redis.publish("cnas:events", json.dumps(event, default=str))


# ── Issue a new ticket (citizen scans / walks up) ────────────────────────────
async def issue_ticket(db: AsyncSession, redis: aioredis.Redis, data: TicketIssue):

    result = await db.execute(
        select(Service).where(Service.id == data.service_id)
    )
    service = result.scalar_one_or_none()

    if not service:
        raise HTTPException(status_code=404, detail="Service not found")

    queue_result = await db.execute(
        select(Queue).where(Queue.service_id == service.id)
    )
    queue = queue_result.scalar_one_or_none()

    if not queue:
        raise HTTPException(status_code=400, detail="No queue found")

    # counter per category
    counter_key = f"cnas:counter:{service.category.value}"
    counter = await redis.incr(counter_key)

    prefix_map = {
        "prestation": "A",
        "medical": "B"
    }

    prefix = prefix_map.get(service.category.value)

    if not prefix:
        raise HTTPException(400, "Unknown category")

    ticket_number = f"{prefix}{str(counter).zfill(3)}"

    # SINGLE MODE QUEUE KEY
    queue_key = build_queue_key(service.category.value)
    print("QUEUE KEY USED:", queue_key)
    ticket = Ticket(
        number=ticket_number,
        service_id=service.id,
        queue_id=queue.id,
        sub_service=data.sub_service,
        agent_id=None,
        status=TicketStatus.waiting,
        priority=bool(data.priority)
    )

    db.add(ticket)
    await db.commit()
    await db.refresh(ticket)

    await push_ticket_to_queue(redis, queue_key, ticket)

    waiting_count = await redis.llen(queue_key)

    await _publish(redis, {
        "type": "ticket_created",
        "ticket_number": ticket.number,
        "service_id": service.id,
        "waiting_count": waiting_count
    })

    return TicketOut(
    id=ticket.id,
    number=ticket.number,
    status=ticket.status,
    wait_minutes=0,
    created_at=ticket.created_at,
    called_at=ticket.called_at,
    service_name=service.name if service else None,
    priority=ticket.priority
 )
# ── Agent action: call next, skip, recall, done ───────────────────────────────
async def agent_action(db: AsyncSession, redis: aioredis.Redis, action: str, agent_id: UUID):
    # Get the agent and their assigned queue
    result = await db.execute(select(Agent).where(Agent.id == agent_id))
    agent = result.scalar_one_or_none()
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    if agent.is_paused and action not in ("resume",):
        raise HTTPException(status_code=400, detail="Agent is paused")

    

    if action == "call_next":
        return await _call_next(db, redis, agent)
    elif action == "skip":
        return await _skip_current(db, redis, agent)
    elif action == "recall":
        return await _recall_current(db, redis, agent)
    elif action == "done":
        return await _mark_done(db, redis, agent)
    elif action == "pause":
        return await _toggle_pause(db, redis, agent, True)
    elif action == "resume":
        return await _toggle_pause(db, redis, agent, False)
    else:
        raise HTTPException(status_code=400, detail=f"Unknown action: {action}")




async def _call_next(db: AsyncSession, redis: aioredis.Redis, agent: Agent):

    result = await db.execute(
        select(Ticket).where(
            Ticket.agent_id == agent.id,
            Ticket.status == TicketStatus.serving
        )
    )
    current = result.scalars().first()

    if current:
        current.status = TicketStatus.done
        current.done_at = datetime.now(timezone.utc)

    # SINGLE MODE QUEUE
    queue_key = build_queue_key(agent.category.value)

    ticket_id = await redis.lpop(queue_key)

    if not ticket_id:
        await db.commit()
        return {"message": "No tickets waiting"}

    ticket_id = ticket_id.decode() if isinstance(ticket_id, bytes) else ticket_id

    result = await db.execute(
        select(Ticket).where(cast(Ticket.id, PG_UUID) == ticket_id)
    )
    next_ticket = result.scalar_one_or_none()

    if not next_ticket:
        await db.commit()
        return {"message": "Ticket not found"}

    now = datetime.now(timezone.utc)

    next_ticket.status = TicketStatus.serving
    next_ticket.agent_id = agent.id
    next_ticket.called_at = now
    next_ticket.started_at = now
    
    from app.core.websocket_manager import manager

    await manager.broadcast_to_room(
    f"ticket:{next_ticket.id}",
    {
        "type": "ticket_called",
        "ticket_id": str(next_ticket.id),
        "ticket_number": next_ticket.number,
        "agent_id": str(agent.id),
        "service": agent.category.value,
    }
)

    await db.commit()


    await redis.set(f"cnas:current:agent:{agent.id}", str(next_ticket.id))
    await redis.set(f"cnas:start:ticket:{next_ticket.id}", str(time.time()))

   
    waiting_count = await redis.llen(queue_key)

    await _publish(redis, {
        "type": "ticket_called",
        "agent_id": str(agent.id),
        "agent_name": agent.name,
        "ticket_number": next_ticket.number,
        "ticket_id": str(next_ticket.id),
        "waiting_count": waiting_count,
    })

    return {
        "ticket_number": next_ticket.number,
        "waiting": waiting_count
    }
async def _skip_current(db: AsyncSession, redis: aioredis.Redis, agent: Agent):
    result = await db.execute(
        select(Ticket).where(
            Ticket.agent_id == agent.id,
            Ticket.status == TicketStatus.serving
        )
    )

    ticket = result.scalars().first()

    if not ticket:
        raise HTTPException(status_code=404, detail="No ticket currently serving")

    ticket.status = TicketStatus.skipped
    ticket.done_at = datetime.now(timezone.utc)

    await db.commit()

    await _publish(redis, {
        "type": "ticket_skipped",
        "agent_id": str(agent.id),
        "ticket_number": ticket.number,
        "ticket_id": str(ticket.id),
    })

    return {"skipped": ticket.number}


async def _recall_current(db: AsyncSession, redis: aioredis.Redis, agent: Agent):
    result = await db.execute(
        select(Ticket).where(Ticket.agent_id == agent.id, Ticket.status == TicketStatus.serving)
    )
    ticket = result.scalars().first()
    if not ticket:
        raise HTTPException(status_code=404, detail="No ticket currently serving")

    await _publish(redis, {
        "type": "ticket_recalled",
        "agent_id": str(agent.id),
        "ticket_number": ticket.number,
        "ticket_id": str(ticket.id),
    })
    return {"recalled": ticket.number}


async def _mark_done(db: AsyncSession, redis: aioredis.Redis, agent: Agent):
    result = await db.execute(
        select(Ticket).where(
            Ticket.agent_id == agent.id,
            Ticket.status == TicketStatus.serving
        )
    )

    ticket = result.scalars().first()

    if not ticket:
        raise HTTPException(status_code=404, detail="No ticket currently serving")

    ticket.status = TicketStatus.done
    ticket.done_at = datetime.now(timezone.utc)

    await db.commit()

    # 🟢 STOP TIMERS
    await redis.delete(f"cnas:start:ticket:{ticket.id}")
    await redis.delete(f"cnas:current:agent:{agent.id}")

    await _publish(redis, {
        "type": "ticket_done",
        "agent_id": str(agent.id),
        "ticket_number": ticket.number,
    })

    return {"done": ticket.number}

async def _toggle_pause(db: AsyncSession, redis: aioredis.Redis, agent: Agent, paused: bool):
    agent.is_paused = paused
    await db.commit()

    await _publish(redis, {
        "type": "agent_paused" if paused else "agent_resumed",
        "agent_id": str(agent.id),
        "agent_name": agent.name,
    })
    return {"paused": paused}


# ── Queue state (for AgentPage sidebar) ──────────────────────────────────────
async def get_queue_for_agent(db: AsyncSession, redis: aioredis.Redis, agent_id: UUID):

    result = await db.execute(
        select(Agent).where(Agent.id == agent_id)
    )

    agent = result.scalar_one_or_none()

    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    queue_key = build_queue_key(agent.category.value)

    queue_ids = await redis.lrange(queue_key, 0, -1)

    if not queue_ids:
        return []

    queue_ids = [
        q.decode() if isinstance(q, bytes) else q
        for q in queue_ids
    ]

    result = await db.execute(
        select(Ticket).where(
            cast(Ticket.id, PG_UUID).in_(queue_ids)
        )
    )

    tickets = result.scalars().all()

    map_t = {str(t.id): t for t in tickets}

    ordered = [map_t[i] for i in queue_ids if i in map_t]

    return [
    {
        "id": t.id,
        "number": t.number,
        "status": t.status,
        "priority": t.priority,
        "created_at": t.created_at,
        "sub_service": t.sub_service,
        "category": t.service.category.value if t.service else None,
        "service_name": t.service.name if t.service else None,
    }
    for t in ordered
]

# ── Stats for admin dashboard ─────────────────────────────────────────────────
async def get_stats(db: AsyncSession):
    from app.schemas.schemas import StatsOut

    agents_count = await db.scalar(select(func.count()).select_from(Agent).where(Agent.is_active == True))
    active_agents = await db.scalar(select(func.count()).select_from(Agent).where(Agent.is_paused == False, Agent.is_active == True))
    waiting = await db.scalar(select(func.count()).select_from(Ticket).where(Ticket.status == TicketStatus.waiting))
    today_total = await db.scalar(select(func.count()).select_from(Ticket).where(func.date(Ticket.created_at) == func.current_date()))
    today_served = await db.scalar(select(func.count()).select_from(Ticket).where(
        Ticket.status == TicketStatus.done,
        func.date(Ticket.done_at) == func.current_date()
    ))

    return StatsOut(
        total_agents=agents_count or 0,
        active_windows=active_agents or 0,
        citizens_waiting=waiting or 0,
        avg_wait_minutes=12.0,   # TODO: compute from real data
        tickets_today=today_total or 0,
        served_today=today_served or 0,
    )
async def get_serving_count_by_agent(db: AsyncSession, agent_id: UUID):
    return await db.scalar(
        select(func.count())
        .select_from(Ticket)
        .where(
            Ticket.agent_id == agent_id,
            Ticket.status == TicketStatus.serving
        )
    )
async def get_ticket_wait_time(redis, ticket_id):
    start = await redis.get(f"cnas:start:ticket:{ticket_id}")
    if not start:
        return 0

    return int(time.time() - float(start))
async def push_ticket_to_queue(
    redis: aioredis.Redis,
    queue_key: str,
    ticket: Ticket
):
    ticket_id = str(ticket.id)

    # ─────────────────────────────
    # store metadata safely
    # ─────────────────────────────
    await redis.set(
        f"cnas:ticket:{ticket_id}",
        json.dumps({"priority": bool(ticket.priority)})
    )

    # ─────────────────────────────
    # NORMAL ticket → push to end
    # ─────────────────────────────
    if not ticket.priority:
        await redis.rpush(queue_key, ticket_id)
        return

    # ─────────────────────────────
    # PRIORITY ticket logic
    # must be placed AFTER last priority ticket
    # ─────────────────────────────
    try:
        queue = await redis.lrange(queue_key, 0, -1)
        queue = [
            q.decode() if isinstance(q, bytes) else q
            for q in queue
        ]

        last_priority = None

        for tid in queue:
            meta = await redis.get(f"cnas:ticket:{tid}")

            if not meta:
                continue

            try:
                meta = json.loads(meta)
            except Exception:
                continue

            if meta.get("priority") is True:
                last_priority = tid

        # ─────────────────────────────
        # INSERT AFTER LAST PRIORITY
        # ─────────────────────────────
        if last_priority:
            await redis.linsert(queue_key, "AFTER", last_priority, ticket_id)
        else:
            # no priority exists → goes to FRONT (important for your rule)
            await redis.lpush(queue_key, ticket_id)

    except Exception as e:
        # 🚨 IMPORTANT: fallback so ticket creation NEVER breaks
        print("QUEUE PRIORITY ERROR:", e)
        await redis.rpush(queue_key, ticket_id)