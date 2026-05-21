#app\routers\tickets.py
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from uuid import UUID
import redis.asyncio as aioredis
from app.models.models import Ticket, Queue, Agent, Service, TicketStatus
from sqlalchemy import select
from app.config import settings
from app.database import get_db
from app.core.redis import get_redis
from app.schemas.schemas import TicketIssue, TicketAction, TicketOut, StatsOut
from app.services import ticket_service
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy import cast
from fastapi import Path
from sqlalchemy import delete
from fastapi import APIRouter, Depends, HTTPException
import redis.asyncio as aioredis
from app.core.redis import get_redis
from app.services.ticket_service import _publish
from sqlalchemy.orm import selectinload

router = APIRouter(prefix="/tickets", tags=["Tickets & Queue"])


# =========================
# ISSUE TICKET
# =========================
@router.post("/issue", response_model=TicketOut, status_code=201)
async def issue_ticket(
    data: TicketIssue,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    return await ticket_service.issue_ticket(db, redis, data)


# =========================
# AGENT ACTIONS
# =========================
@router.post("/action")
async def agent_action(
    data: TicketAction,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    return await ticket_service.agent_action(
        db, redis, data.action, data.agent_id
    )


# =========================
# QUEUE BY AGENT
# =========================
@router.get("/queue/agent/{agent_id}")
async def get_agent_queue(
    agent_id: UUID,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    return await ticket_service.get_queue_for_agent(db, redis, agent_id)


# =========================
# QUEUE BY CATEGORY
# =========================
@router.get("/queue/{category}")
async def get_category_queue(
    category: str,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    queue_key = f"cnas:queue:{category}"

    queue_ids = await redis.lrange(queue_key, 0, -1)

    if not queue_ids:
        return []

    queue_ids = [
        q.decode() if isinstance(q, bytes) else q
        for q in queue_ids
    ]

    result = await db.execute(
        select(Ticket)
        .options(selectinload(Ticket.service))
        .where(cast(Ticket.id, PG_UUID).in_(queue_ids))
    )

    tickets = result.scalars().all()

    tickets_map = {str(t.id): t for t in tickets}

    ordered = [
        tickets_map[t_id]
        for t_id in queue_ids
        if t_id in tickets_map
    ]

    return [
        {
            "id": t.id,
            "number": t.number,
            "status": t.status.value if hasattr(t.status, "value") else t.status,
            "priority": t.priority,
            "created_at": t.created_at,
            "sub_service": t.sub_service,
            "category": t.service.category.value if t.service else None,
            "service_name": t.service.name if t.service else None,
        }
        for t in ordered
    ]


# =========================
# CURRENT SERVING TICKET
# =========================
@router.get("/current/{agent_id}")
async def get_current_ticket(
    agent_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Ticket).where(
            Ticket.agent_id == agent_id,
            Ticket.status == TicketStatus.serving
        )
    )
    ticket = result.scalars().first()
    if not ticket:
        return None
    return {
        "id": str(ticket.id),
        "number": ticket.number,
        "status": ticket.status.value,
        "priority": ticket.priority,
        "sub_service": ticket.sub_service,
        "created_at": str(ticket.created_at),
    }


# =========================
# CANCEL TICKET
# =========================
@router.delete("/{ticket_id}")
async def cancel_ticket(
    ticket_id: UUID,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    result = await db.execute(
        select(Ticket).where(Ticket.id == ticket_id)
    )

    ticket = result.scalar_one_or_none()

    if not ticket:
        raise HTTPException(status_code=404, detail="Ticket not found")

    # remove from redis queue
    service_result = await db.execute(
        select(Service).where(Service.id == ticket.service_id)
    )
    service = service_result.scalar_one_or_none()

    if service:
        queue_key = f"cnas:queue:{service.category.value}"
        await redis.lrem(queue_key, 0, str(ticket.id))

    # delete from DB
    await db.delete(ticket)
    await db.commit()

    await _publish(redis, {
        "type": "ticket_cancelled",
        "ticket_id": str(ticket_id),
        "ticket_number": ticket.number
    })

    return {"message": "ticket cancelled"}