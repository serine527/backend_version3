#app\routers\system.py
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.services import system_service

router = APIRouter(prefix="/system", tags=["System"])


@router.get("/")
async def get_config(db: AsyncSession = Depends(get_db)):
    return await system_service.get_config(db)


@router.patch("/mode")
async def update_mode(mode: str, db: AsyncSession = Depends(get_db)):
    return await system_service.update_mode(db, mode)