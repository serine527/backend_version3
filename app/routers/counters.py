#app\routers\counters.py
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.services import counter_service
from app.schemas.schemas import CounterOut
from typing import List
from app.schemas.schemas import CounterCreate
from pydantic import BaseModel



router = APIRouter(prefix="/counters", tags=["Counters"])

@router.get("/", response_model=list[CounterOut])
async def list_counters(db: AsyncSession = Depends(get_db)):
    return await counter_service.get_counters(db)

@router.post("/")
async def add_counter(data: CounterCreate, db: AsyncSession = Depends(get_db)):
   return await counter_service.create_counter(db, data.name)

@router.delete("/{counter_id}")
async def remove_counter(counter_id: int, db: AsyncSession = Depends(get_db)):
    return await counter_service.delete_counter(db, counter_id)


@router.put("/{counter_id}/assign")
async def assign_counter(
    counter_id: int,
    agent_id: str,
    db: AsyncSession = Depends(get_db)
):
    return await counter_service.assign_counter(db, counter_id, agent_id)

class CounterStatusUpdate(BaseModel):
    is_active: bool

@router.patch("/{counter_id}/status")
async def update_counter_status(
    counter_id: int,
    data: CounterStatusUpdate,
    db: AsyncSession = Depends(get_db)
):
    return await counter_service.set_counter_status(
        db,
        counter_id,
        data.is_active
    )