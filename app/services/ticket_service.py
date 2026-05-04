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


def build_queue_key(service_category: str, sub_service: str | None):
    if settings.QUEUE_MODE == "single":
        return f"cnas:queue:{service_category}"

    return f"cnas:queue:{service_category}:{sub_service or 'default'}"

async def select_best_agent(db, redis, service, sub_service):
    query = select(Agent).where(
        Agent.is_active == True,
        Agent.is_paused == False,
        Agent.category == service.category   # ✅ enforce category
    )

    result = await db.execute(query)
    agents = result.scalars().all()

    if not agents:
        raise HTTPException(status_code=400, detail="No agents available")

    # MULTI MODE → strict matching
    if settings.QUEUE_MODE == "multi":
        agents = [
            a for a in agents
            if a.assigned_service
            and a.assigned_service.strip() == sub_service.strip()
        ]

        if not agents:
            raise HTTPException(status_code=400, detail="No agents for this service")

    # pick least loaded agent
    best_agent = None
    min_queue = float("inf")

    for agent in agents:
        q_len = await redis.llen(f"cnas:queue:agent:{agent.id}")

        if q_len < min_queue:
            min_queue = q_len
            best_agent = agent

    return best_agent
# ── Redis key helpers ─────────────────────────────────────────────────────────
 
def _stats_key() -> str:
    return "cnas:stats"


# ── Publish event to Redis pub/sub ────────────────────────────────────────────
async def _publish(redis: aioredis.Redis, event: dict):
    await redis.publish("cnas:events", json.dumps(event, default=str))


# ── Issue a new ticket (citizen scans / walks up) ────────────────────────────
async def issue_ticket(db: AsyncSession, redis: aioredis.Redis, data: TicketIssue):

    # 1. Get service
    result = await db.execute(
        select(Service).where(Service.id == data.service_id)
    )
    service = result.scalar_one_or_none()

    if not service:
        raise HTTPException(status_code=404, detail="Service not found")

    # 2. Pick agent only in MULTI mode
    agent = None
    if settings.QUEUE_MODE == "multi":
        agent = await select_best_agent(db, redis, service, data.sub_service)

    # 3. Get queue config
    queue_result = await db.execute(
        select(Queue).where(Queue.service_id == service.id)
    )
    queue = queue_result.scalar_one_or_none()

    if not queue:
        raise HTTPException(status_code=400, detail="No queue found")

    # 4. Generate ticket number
    counter = await redis.incr(f"cnas:counter:service:{service.id}")
    ticket_number = f"{queue.prefix}{str(counter).zfill(3)}"

    # 5. Build queue key (ONLY ONCE)
    queue_key = build_queue_key(service.category.value, data.sub_service)

    print("QUEUE KEY (ISSUE):", queue_key)

    # 6. Create ticket (DB)
    ticket = Ticket(
        number=ticket_number,
        service_id=service.id,
        queue_id=queue.id,
        sub_service=data.sub_service,
        agent_id=agent.id if agent else None,
        status=TicketStatus.waiting,
        priority=bool(data.priority)
    )

    db.add(ticket)
    await db.commit()
    await db.refresh(ticket)

    # 7. Push to Redis queue
    await push_ticket_to_queue(redis, queue_key, ticket)

    # 8. Get waiting count
    waiting_count = await redis.llen(queue_key)

    # 9. Publish event (WebSocket update)
    await _publish(redis, {
        "type": "ticket_created",
        "agent_id": str(agent.id) if agent else None,
        "ticket_number": ticket.number,
        "service_id": service.id,
        "waiting_count": waiting_count
    })

    # 10. Return response
    return TicketOut(
        id=ticket.id,
        number=ticket.number,
        status=ticket.status,
        wait_minutes=ticket.wait_minutes,
        created_at=ticket.created_at,
        called_at=ticket.called_at,
        service_name=service.name,
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

    # finish current ticket
    result = await db.execute(
        select(Ticket).where(
            Ticket.agent_id == agent.id,
            Ticket.status == TicketStatus.serving
        )
    )
    current = result.scalar_one_or_none()

    if current:
        current.status = TicketStatus.done
        current.done_at = datetime.now(timezone.utc)

    # queue key
    queue_key = build_queue_key(
        agent.category.value,
        agent.sub_service or agent.assigned_service
    )

    # next ticket
    ticket_id = await redis.lpop(queue_key)

    if not ticket_id:
        await db.commit()
        return {"message": "No tickets waiting"}

    ticket_id = ticket_id.decode() if isinstance(ticket_id, bytes) else ticket_id

    # FIX: safe UUID match
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

    await db.commit()

    # runtime tracking
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

    ticket = result.scalar_one_or_none()

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
    ticket = result.scalar_one_or_none()
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

    ticket = result.scalar_one_or_none()

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

    result = await db.execute(select(Agent).where(Agent.id == agent_id))
    agent = result.scalar_one_or_none()

    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    # SINGLE MODE
    if settings.QUEUE_MODE == "single":
        queue_key = f"cnas:queue:category:{agent.category}"
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

        return [TicketOut.model_validate(t) for t in ordered]

    # MULTI MODE
    result = await db.execute(
        select(Ticket)
        .where(
            Ticket.agent_id == agent.id,
            Ticket.status.in_([TicketStatus.waiting, TicketStatus.serving])
        )
        .order_by(Ticket.priority.desc(), Ticket.created_at.asc())
    )

    return [TicketOut.model_validate(t) for t in result.scalars().all()]

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
async def push_ticket_to_queue(redis: aioredis.Redis, queue_key: str, ticket: Ticket):
    ticket_id = str(ticket.id)

    # store metadata
    await redis.set(
        f"cnas:ticket:{ticket_id}",
        json.dumps({"priority": bool(ticket.priority)})
    )

    # NORMAL ticket → goes to end
    if not ticket.priority:
        await redis.rpush(queue_key, ticket_id)
        return

    # PRIORITY ticket logic
    queue = await redis.lrange(queue_key, 0, -1)
    queue = [q.decode() if isinstance(q, bytes) else q for q in queue]

    last_priority = None

    for tid in queue:
        meta = await redis.get(f"cnas:ticket:{tid}")
        if meta:
            meta = json.loads(meta)
            if meta.get("priority"):
                last_priority = tid

    # insert logic
    if last_priority:
        await redis.linsert(queue_key, "AFTER", last_priority, ticket_id)
    else:
        # if no priority exists → still keep fairness after current serving logic
        await redis.rpush(queue_key, ticket_id)