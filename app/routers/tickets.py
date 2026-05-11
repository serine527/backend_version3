#app\routers\tickets.py
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from uuid import UUID
import redis.asyncio as aioredis
from app.models.models import Ticket, Queue, Agent, Service
from sqlalchemy import select
from app.config import settings
from app.database import get_db
from app.core.redis import get_redis
from app.schemas.schemas import TicketIssue, TicketAction, TicketOut, StatsOut
from app.services import ticket_service
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from app.models.models import Ticket, Queue
from sqlalchemy import cast
from fastapi import Path
from sqlalchemy import delete
from fastapi import APIRouter, Depends, HTTPException
import redis.asyncio as aioredis
from app.core.redis import get_redis
from app.services.ticket_service import _publish


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
# STATS
# =========================

async def get_my_queue(
    category: str,
    sub_service: str,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):

    # 1. Build Redis key
    queue_key = f"cnas:queue:{category}:{sub_service}"

    # 2. Get queue from Redis
    queue_ids = await redis.lrange(queue_key, 0, -1)

    queue_ids = [
    q.decode() if isinstance(q, bytes) else q
    for q in queue_ids
]

    if not queue_ids:
        return []

    # 3. Fetch tickets from DB
    result = await db.execute(
        select(Ticket).where(Ticket.id.in_(queue_ids))
    )

    tickets = result.scalars().all()

    tickets_map = {str(t.id): t for t in tickets}

    # 4. Preserve Redis order
    ordered = [
        tickets_map[t_id]
        for t_id in queue_ids
        if t_id in tickets_map
    ]

    # 5. Return response
    return [
        {
            "id": t.id,
            "number": t.number,
            "status": t.status,
            "sub_service": t.sub_service,
            "priority": t.priority,
            "created_at": t.created_at,
        }
        for t in ordered
    ]
@router.get("/queue/agent/{agent_id}")
async def get_agent_queue(
    agent_id: UUID,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    return await ticket_service.get_queue_for_agent(db, redis, agent_id)

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
        select(Ticket).where(
            cast(Ticket.id, PG_UUID).in_(queue_ids)
        )
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
            "status": t.status,
            "priority": t.priority,
            "created_at": t.created_at,
        }
        for t in ordered
    ]

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