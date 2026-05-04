from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from fastapi import HTTPException
from app.models.models import Service
from app.schemas.schemas import ServiceCreate


async def get_services(db: AsyncSession):
    result = await db.execute(select(Service).where(Service.is_active == True))
    return result.scalars().all()


async def create_service(db: AsyncSession, data: ServiceCreate):
    service = Service(
        name=data.name,
        category=data.category,
        avg_time_min=data.avg_time_min,
    )
    db.add(service)
    await db.commit()
    await db.refresh(service)
    return service


async def delete_service(db: AsyncSession, service_id: int):
    result = await db.execute(select(Service).where(Service.id == service_id))
    service = result.scalar_one_or_none()

    if not service:
        raise HTTPException(status_code=404, detail="Service not found")

    service.is_active = False
    await db.commit()
    return {"message": "Service deleted"}