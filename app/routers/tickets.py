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

from app.models.models import Ticket, Queue

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
@router.get("/stats", response_model=StatsOut)
async def get_stats(db: AsyncSession = Depends(get_db)):
    return await ticket_service.get_stats(db)


# =========================
# SERVICE QUEUE (FIXED)
# =========================
@router.get("/queue/{category}/{sub_service}")
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