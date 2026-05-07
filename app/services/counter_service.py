from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from fastapi import HTTPException
from app.models.models import Counter

async def get_counters(db: AsyncSession):
    result = await db.execute(select(Counter).where(Counter.is_active == True))
    return result.scalars().all()


async def create_counter(db: AsyncSession, name: str, number: int):
    counter = Counter(name=name, number=number)
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