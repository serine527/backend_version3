from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.services import counter_service

router = APIRouter(prefix="/counters", tags=["Counters"])

@router.get("/")
async def list_counters(db: AsyncSession = Depends(get_db)):
    return await counter_service.get_counters(db)

@router.post("/")
async def add_counter(name: str, number: int, db: AsyncSession = Depends(get_db)):
    return await counter_service.create_counter(db, name, number)

@router.delete("/{counter_id}")
async def remove_counter(counter_id: int, db: AsyncSession = Depends(get_db)):
    return await counter_service.delete_counter(db, counter_id)