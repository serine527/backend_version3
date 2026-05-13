#app\main.py
import asyncio
import logging
from app.routers import debug
from contextlib import asynccontextmanager
from app.routers import services, system, stats
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import counters
from app.database import engine, Base, AsyncSessionLocal
from app.models import models
from app.core.redis import init_redis, close_redis
from app.core.websocket_manager import redis_subscriber_loop
from app.routers import auth, agents, tickets, websockets
from app.services.auth_service import create_admin

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting CNAS backend…")

    # ======================
    # DATABASE INIT
    # ======================
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Admin seed
    async with AsyncSessionLocal() as db:
        await create_admin(db, username="admin", password="admin1234")

    subscriber_task = None

    # ======================
    # REDIS INIT
    # ======================
    try:
        await init_redis()
        logger.info("Redis connected")

        subscriber_task = asyncio.create_task(redis_subscriber_loop())

    except Exception as e:
        logger.error(f"Redis init failed: {e}")

    yield

    # ======================
    # SHUTDOWN
    # ======================
    if subscriber_task:
        subscriber_task.cancel()
        try:
            await subscriber_task
        except asyncio.CancelledError:
            pass

    try:
        await close_redis()
    except Exception as e:
        logger.error(f"Redis shutdown error: {e}")

    await engine.dispose()
    logger.info("CNAS backend shut down cleanly")


app = FastAPI(
    title="CNAS Queue Management API",
    description="Backend for the CNAS queue management system — REST + WebSocket",
    version="2.0.0",
    lifespan=lifespan,
)

# =========================
# CORS CONFIG (safe baseline)
# =========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",   # ✅ your frontend
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,        # ✅ IMPORTANT
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(auth.router, prefix="/api/v1")
app.include_router(agents.router, prefix="/api/v1")
app.include_router(tickets.router, prefix="/api/v1")
app.include_router(websockets.router)
app.include_router(services.router, prefix="/api/v1")
app.include_router(system.router, prefix="/api/v1")
app.include_router(stats.router, prefix="/api/v1")
app.include_router(counters.router, prefix="/api/v1")
app.include_router(debug.router, prefix="/api/v1")

@app.get("/", tags=["Health"])
async def health():
    return {"status": "ok", "service": "CNAS Queue API v2"}