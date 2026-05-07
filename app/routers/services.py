#app\routers\services.py
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from app.database import get_db
from app.schemas.schemas import ServiceCreate, ServiceOut
from app.services import service_service

router = APIRouter(prefix="/services", tags=["Services"])


@router.get("/", response_model=List[ServiceOut])
async def list_services(db: AsyncSession = Depends(get_db)):
    return await service_service.get_services(db)


@router.post("/", response_model=ServiceOut)
async def create_service(data: ServiceCreate, db: AsyncSession = Depends(get_db)):
    return await service_service.create_service(db, data)


@router.delete("/{service_id}")
async def delete_service(service_id: int, db: AsyncSession = Depends(get_db)):
    print("SERVICE DELETE CALLED:", service_id)
    return await service_service.delete_service(db, service_id)