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
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update
from fastapi import HTTPException
import redis.asyncio as aioredis

from app.models.models import Ticket, Queue, Agent, Service, TicketStatus
from app.schemas.schemas import TicketIssue, TicketOut


# ── Redis key helpers ─────────────────────────────────────────────────────────
def _queue_key(queue_id: int) -> str:
    return f"cnas:queue:{queue_id}"

def _stats_key() -> str:
    return "cnas:stats"


# ── Publish event to Redis pub/sub ────────────────────────────────────────────
async def _publish(redis: aioredis.Redis, event: dict):
    await redis.publish("cnas:events", json.dumps(event, default=str))


# ── Issue a new ticket (citizen scans / walks up) ────────────────────────────
async def issue_ticket(db: AsyncSession, redis: aioredis.Redis, data: TicketIssue) -> TicketOut:
    # Get the queue for this service
    result = await db.execute(select(Queue).where(Queue.service_id == data.service_id))
    queue = result.scalar_one_or_none()
    if not queue:
        raise HTTPException(status_code=404, detail="Queue not found for this service")

    # Increment ticket counter atomically in Redis
    counter = await redis.incr(f"cnas:counter:{queue.id}")
    ticket_number = f"{queue.prefix}{str(counter).zfill(3)}"

    # Estimate wait time: waiting count × avg service time
    waiting_count = await redis.llen(_queue_key(queue.id))
    service_result = await db.execute(select(Service).where(Service.id == data.service_id))
    service = service_result.scalar_one_or_none()
    wait_minutes = int(waiting_count * (service.avg_time_min if service else 10))

    ticket = Ticket(
        number=ticket_number,
        queue_id=queue.id,
        status=TicketStatus.waiting,
        wait_minutes=wait_minutes,
    )
    db.add(ticket)
    await db.commit()
    await db.refresh(ticket)

    # Push ticket ID into Redis list (the queue)
    await redis.rpush(_queue_key(queue.id), str(ticket.id))

    # Broadcast to admin + relevant agent rooms
    await _publish(redis, {
        "type": "queue_updated",
        "queue_id": queue.id,
        "service_id": data.service_id,
        "ticket_number": ticket_number,
        "waiting_count": waiting_count + 1,
        "wait_minutes": wait_minutes,
    })

    return TicketOut.model_validate(ticket)


# ── Agent action: call next, skip, recall, done ───────────────────────────────
async def agent_action(db: AsyncSession, redis: aioredis.Redis, action: str, agent_id: UUID):
    # Get the agent and their assigned queue
    result = await db.execute(select(Agent).where(Agent.id == agent_id))
    agent = result.scalar_one_or_none()
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    if agent.is_paused and action not in ("resume",):
        raise HTTPException(status_code=400, detail="Agent is paused")

    # Find the queue for this agent's service
    queue_id = await _get_agent_queue_id(db, agent)

    if action == "call_next":
        return await _call_next(db, redis, agent, queue_id)
    elif action == "skip":
        return await _skip_current(db, redis, agent, queue_id)
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


async def _get_agent_queue_id(db: AsyncSession, agent: Agent) -> int:
    if not agent.assigned_service:
        # In single-window mode, we find any available queue (first one)
        result = await db.execute(select(Queue).limit(1))
        queue = result.scalar_one_or_none()
    else:
        # In multi-window mode, find the queue for the agent's assigned service
        result = await db.execute(
            select(Queue).join(Service).where(Service.name == agent.assigned_service)
        )
        queue = result.scalar_one_or_none()

    if not queue:
        raise HTTPException(status_code=404, detail="No queue found for this agent")
    return queue.id


async def _call_next(db: AsyncSession, redis: aioredis.Redis, agent: Agent, queue_id: int):
    # Mark any currently serving ticket as done
    result = await db.execute(
        select(Ticket).where(Ticket.agent_id == agent.id, Ticket.status == TicketStatus.serving)
    )
    current = result.scalar_one_or_none()
    if current:
        current.status = TicketStatus.done
        current.done_at = datetime.now(timezone.utc)

    # Pop next ticket from Redis queue
    ticket_id = await redis.lpop(_queue_key(queue_id))
    if not ticket_id:
        await db.commit()
        return {"message": "No tickets waiting"}

    # Update ticket in DB
    result = await db.execute(select(Ticket).where(Ticket.id == ticket_id))
    next_ticket = result.scalar_one_or_none()
    if not next_ticket:
        return {"message": "Ticket not found"}

    next_ticket.status = TicketStatus.serving
    next_ticket.agent_id = agent.id
    next_ticket.called_at = datetime.now(timezone.utc)
    await db.commit()

    waiting_count = await redis.llen(_queue_key(queue_id))

    await _publish(redis, {
        "type": "ticket_called",
        "agent_id": str(agent.id),
        "agent_name": agent.name,
        "ticket_number": next_ticket.number,
        "ticket_id": str(next_ticket.id),
        "queue_id": queue_id,
        "waiting_count": waiting_count,
    })

    return {"ticket_number": next_ticket.number, "waiting": waiting_count}


async def _skip_current(db: AsyncSession, redis: aioredis.Redis, agent: Agent, queue_id: int):
    result = await db.execute(
        select(Ticket).where(Ticket.agent_id == agent.id, Ticket.status == TicketStatus.serving)
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
        select(Ticket).where(Ticket.agent_id == agent.id, Ticket.status == TicketStatus.serving)
    )
    ticket = result.scalar_one_or_none()
    if not ticket:
        raise HTTPException(status_code=404, detail="No ticket currently serving")

    ticket.status = TicketStatus.done
    ticket.done_at = datetime.now(timezone.utc)
    await db.commit()

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
async def get_queue_for_agent(db: AsyncSession, agent_id: UUID):
    result = await db.execute(select(Agent).where(Agent.id == agent_id))
    agent = result.scalar_one_or_none()
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    queue_id = await _get_agent_queue_id(db, agent)

    result = await db.execute(
        select(Ticket)
        .where(Ticket.queue_id == queue_id, Ticket.status.in_([TicketStatus.waiting, TicketStatus.serving]))
        .order_by(Ticket.created_at)
    )
    tickets = result.scalars().all()
    return [TicketOut.model_validate(t) for t in tickets]


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
