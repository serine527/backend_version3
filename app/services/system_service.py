#app\services\system_service.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.models import SystemConfig


async def get_config(db: AsyncSession):
    result = await db.execute(select(SystemConfig).limit(1))
    config = result.scalar_one_or_none()

    # ✅ AUTO CREATE IF NOT EXISTS
    if not config:
        config = SystemConfig(mode="single")
        db.add(config)
        await db.commit()
        await db.refresh(config)

    return config


async def update_mode(db: AsyncSession, mode: str):
    result = await db.execute(select(SystemConfig).limit(1))
    config = result.scalar_one_or_none()

    if not config:
        config = SystemConfig(mode=mode)
        db.add(config)
    else:
        config.mode = mode

    await db.commit()
    await db.refresh(config)
    return config