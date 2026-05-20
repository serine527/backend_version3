#app\services\counter_service.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from fastapi import HTTPException
from app.models.models import Counter
from app.models.models import Agent
from sqlalchemy import select, func 
from uuid import UUID


async def get_counters(db: AsyncSession):
    result = await db.execute(select(Counter))
    return result.scalars().all()

async def create_counter(db: AsyncSession, name: str):
    # 🔢 Get current max counter number
    result = await db.execute(select(func.max(Counter.number)))
    max_number = result.scalar() or 0

    # ➕ Create next number automatically
    counter = Counter(name=name, number=max_number + 1)

    db.add(counter)
    await db.commit()
    await db.refresh(counter)

    return counter


async def delete_counter(db: AsyncSession, counter_id: int):
    result = await db.execute(select(Counter).where(Counter.id == counter_id))
    counter = result.scalar_one_or_none()

    if not counter:
        raise HTTPException(status_code=404, detail="Counter not found")

    await db.delete(counter)
    await db.commit()

    return {"message": "Counter deleted"}

async def assign_counter(db: AsyncSession, counter_id: int, agent_id: str):
    agent_uuid = UUID(agent_id)

    counter = await db.execute(select(Counter).where(Counter.id == counter_id))
    counter = counter.scalar_one_or_none()

    agent = await db.execute(select(Agent).where(Agent.id == agent_uuid))
    agent = agent.scalar_one_or_none()

    counter.agent_id = agent_uuid
    await db.commit()
    await db.refresh(counter)

    return counter

async def set_counter_status(db: AsyncSession, counter_id: int, is_active: bool):
    result = await db.execute(
        select(Counter).where(Counter.id == counter_id)
    )
    counter = result.scalar_one_or_none()

    if not counter:
        raise HTTPException(status_code=404, detail="Counter not found")

    counter.is_active = is_active

    await db.commit()
    await db.refresh(counter)

    return counter