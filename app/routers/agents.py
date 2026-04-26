from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from uuid import UUID
from app.database import get_db
from app.schemas.schemas import AgentCreate, AgentUpdate, AgentOut, AgentPasswordChange
from app.services import agent_service

router = APIRouter(prefix="/agents", tags=["Agents"])

@router.get("/", response_model=List[AgentOut])
async def list_agents(db: AsyncSession = Depends(get_db)):
    return await agent_service.get_all_agents(db)

@router.get("/{agent_id}", response_model=AgentOut)
async def get_agent(agent_id: UUID, db: AsyncSession = Depends(get_db)):
    return await agent_service.get_agent(db, agent_id)

@router.post("/", response_model=AgentOut, status_code=201)
async def create_agent(data: AgentCreate, db: AsyncSession = Depends(get_db)):
    return await agent_service.create_agent(db, data)

@router.patch("/{agent_id}", response_model=AgentOut)
async def update_agent(agent_id: UUID, data: AgentUpdate, db: AsyncSession = Depends(get_db)):
    return await agent_service.update_agent(db, agent_id, data)

@router.delete("/{agent_id}")
async def delete_agent(agent_id: UUID, db: AsyncSession = Depends(get_db)):
    return await agent_service.delete_agent(db, agent_id)

@router.post("/{agent_id}/change-password")
async def change_password(agent_id: UUID, data: AgentPasswordChange, db: AsyncSession = Depends(get_db)):
    return await agent_service.change_password(db, agent_id, data)
