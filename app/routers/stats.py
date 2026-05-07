#app\routers\stats.py
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.services import stats_service

router = APIRouter(prefix="/stats", tags=["Stats"])


@router.get("/")
async def get_stats(db: AsyncSession = Depends(get_db)):
    return await stats_service.get_stats(db)