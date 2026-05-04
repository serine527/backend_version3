#app\core\redis.py
import redis.asyncio as aioredis
from app.config import settings
import logging

logger = logging.getLogger(__name__)

redis_client: aioredis.Redis | None = None


async def init_redis():
    global redis_client

    try:
        redis_client = aioredis.from_url(
            settings.REDIS_URL,
            decode_responses=True,

            # stability settings
            socket_connect_timeout=5,
            socket_timeout=30,
            retry_on_timeout=True,
            health_check_interval=10,

            # 🔥 IMPORTANT FIX
            max_connections=20,
        )

        await redis_client.ping()
        logger.info("Redis connected successfully")

    except Exception as e:
     logger.error(f"Redis connection failed: {e}")
    raise RuntimeError("Redis is required for this system to work")


def get_redis() -> aioredis.Redis:
    """
    FastAPI dependency.
    NOTE: not async because it only returns existing instance.
    """
    if redis_client is None:
        raise RuntimeError(
            "Redis is not initialized. Check init_redis() at startup."
        )
    return redis_client


async def close_redis():
    global redis_client

    if redis_client:
        try:
            await redis_client.close()

            pool = getattr(redis_client, "connection_pool", None)
            if pool:
                await pool.disconnect()

            logger.info("Redis connection closed")

        except Exception as e:
            logger.error(f"Error closing Redis: {e}")

        redis_client = None