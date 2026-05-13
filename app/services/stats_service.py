from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from app.models import Agent, Counter, Ticket, TicketStatus

async def get_stats(db: AsyncSession):
    # Total agents
    total_agents = await db.scalar(
        select(func.count()).select_from(Agent)
    )

    # Active counters
    active_counters = await db.scalar(
        select(func.count()).select_from(Counter).where(Counter.is_active == True)
    )

    # Waiting citizens
    waiting_citizens = await db.scalar(
        select(func.count()).select_from(Ticket).where(Ticket.status == TicketStatus.waiting)
    )

    # Average wait time
    avg_wait_time = await db.scalar(
        select(
            func.avg(
                func.extract("epoch", func.now() - Ticket.created_at) / 60
            )
        ).where(Ticket.status == TicketStatus.waiting)
    )

    return {
        "total_agents": total_agents or 0,
        "active_counters": active_counters or 0,
        "waiting_citizens": waiting_citizens or 0,
        "avg_wait_time_min": round(float(avg_wait_time or 0), 1),
    }