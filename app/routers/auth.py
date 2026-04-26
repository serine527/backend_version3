from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.schemas.schemas import LoginRequest, LoginResponse
from app.services import auth_service

router = APIRouter(prefix="/auth", tags=["Auth"])

@router.post("/login", response_model=LoginResponse)
async def login(data: LoginRequest, db: AsyncSession = Depends(get_db)):
    return await auth_service.login(db, data)
