#app\services\agent_service.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from uuid import UUID
from fastapi import HTTPException
from app.models.models import Agent, User, UserRole
from app.schemas.schemas import AgentCreate, AgentUpdate, AgentPasswordChange
from app.core.auth import hash_password, verify_password


async def get_all_agents(db: AsyncSession):
    result = await db.execute(select(Agent).where(Agent.is_active == True))
    return result.scalars().all()


async def get_agent(db: AsyncSession, agent_id: UUID):
    result = await db.execute(select(Agent).where(Agent.id == agent_id))
    agent = result.scalar_one_or_none()
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent


async def create_agent(db: AsyncSession, data: AgentCreate):
    """Create a User login + an Agent profile in one transaction."""
    # Check username uniqueness
    existing = await db.execute(select(User).where(User.username == data.name))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Agent name already exists")

    user = User(
        username=data.name,
        password=hash_password(data.password),
        role=UserRole.agent,
    )
    db.add(user)
    await db.flush()   # get user.id without committing

    agent = Agent(
        user_id=user.id,
        name=data.name,
        last_name=data.last_name,
    )
    db.add(agent)
    await db.commit()
    await db.refresh(agent)
    return agent


async def update_agent(db: AsyncSession, agent_id: UUID, data: AgentUpdate):
    agent = await get_agent(db, agent_id)
    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(agent, field, value)
    await db.commit()
    await db.refresh(agent)
    return agent


async def delete_agent(db: AsyncSession, agent_id: UUID):
    agent = await get_agent(db, agent_id)
    agent.is_active = False   # soft delete — keeps history
    await db.commit()
    return {"message": "Agent deactivated"}


async def change_password(db: AsyncSession, agent_id: UUID, data: AgentPasswordChange):
    agent = await get_agent(db, agent_id)
    if not agent.user_id:
        raise HTTPException(status_code=400, detail="No user account linked")
    result = await db.execute(select(User).where(User.id == agent.user_id))
    user = result.scalar_one_or_none()
    if not user or not verify_password(data.current_password, user.password):
        raise HTTPException(status_code=401, detail="كلمة المرور الحالية غير صحيحة")
    user.password = hash_password(data.new_password)
    await db.commit()
    return {"message": "Password updated"}
