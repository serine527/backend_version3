from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from fastapi import HTTPException, status
from app.models.models import User, Agent, UserRole
from app.schemas.schemas import LoginRequest, LoginResponse
from app.core.auth import verify_password, create_access_token, hash_password


async def login(db: AsyncSession, data: LoginRequest) -> LoginResponse:
    result = await db.execute(select(User).where(User.username == data.username))
    user = result.scalar_one_or_none()

    if not user or not verify_password(data.password, user.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="اسم المستخدم أو كلمة المرور غير صحيحة"
        )

    agent_id = None
    if user.role == UserRole.agent:
        result = await db.execute(select(Agent).where(Agent.user_id == user.id))
        agent = result.scalar_one_or_none()
        if agent:
            agent_id = agent.id

    token = create_access_token({"sub": str(user.id), "role": user.role, "username": user.username})

    return LoginResponse(
        access_token=token,
        role=user.role,
        agent_id=agent_id,
        username=user.username,
    )


async def create_admin(db: AsyncSession, username: str, password: str):
    """Called once at startup to seed the admin account if it doesn't exist."""
    result = await db.execute(select(User).where(User.username == username))
    if result.scalar_one_or_none():
        return
    admin = User(username=username, password=hash_password(password), role=UserRole.admin)
    db.add(admin)
    await db.commit()
