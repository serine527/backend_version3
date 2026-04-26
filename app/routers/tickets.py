from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from uuid import UUID
import redis.asyncio as aioredis

from app.database import get_db
from app.core.redis import get_redis
from app.schemas.schemas import TicketIssue, TicketAction, TicketOut, StatsOut
from app.services import ticket_service

router = APIRouter(prefix="/tickets", tags=["Tickets & Queue"])

@router.post("/issue", response_model=TicketOut, status_code=201)
async def issue_ticket(
    data: TicketIssue,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """A citizen requests a ticket for a service."""
    return await ticket_service.issue_ticket(db, redis, data)

@router.post("/action")
async def agent_action(
    data: TicketAction,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """
    Agent performs an action on their queue.
    Actions: call_next | skip | recall | done | pause | resume
    """
    return await ticket_service.agent_action(db, redis, data.action, data.agent_id)

@router.get("/queue/{agent_id}", response_model=List[TicketOut])
async def get_queue(agent_id: UUID, db: AsyncSession = Depends(get_db)):
    """Get the current queue for an agent (used on AgentPage load)."""
    return await ticket_service.get_queue_for_agent(db, agent_id)

@router.get("/stats", response_model=StatsOut)
async def get_stats(db: AsyncSession = Depends(get_db)):
    """Admin dashboard stats cards."""
    return await ticket_service.get_stats(db)
