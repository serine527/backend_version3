# app/routers/debug.py
from fastapi import APIRouter, Depends
import redis.asyncio as aioredis
from app.core.redis import get_redis

router = APIRouter(prefix="/debug", tags=["Debug"])

@router.delete("/reset-redis")
async def reset(redis: aioredis.Redis = Depends(get_redis)):
    await redis.flushdb()
    return {"message": "Redis cleared"}