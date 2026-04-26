import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.database import engine, Base
from app.models import models  # register all models
from app.core.redis import init_redis, close_redis
from app.core.websocket_manager import redis_subscriber_loop
from app.routers import auth, agents, tickets, websockets
from app.services.auth_service import create_admin
from app.database import AsyncSessionLocal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────────────────
    logger.info("Starting CNAS backend…")

    # 1. Create all DB tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # 2. Seed the default admin account
    async with AsyncSessionLocal() as db:
        await create_admin(db, username="admin", password="admin1234")

    # 3. Connect to Redis
    await init_redis()

    # 4. Start the Redis pub/sub subscriber in the background
    #    This is what feeds real-time events into the WebSocket manager
    subscriber_task = asyncio.create_task(redis_subscriber_loop())
    logger.info("Redis subscriber started")

    yield

    # ── Shutdown ─────────────────────────────────────────────────────────────
    subscriber_task.cancel()
    await close_redis()
    await engine.dispose()
    logger.info("CNAS backend shut down cleanly")


app = FastAPI(
    title="CNAS Queue Management API",
    description="Backend for the CNAS queue management system — REST + WebSocket",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],  # Vite / CRA dev servers
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routes ────────────────────────────────────────────────────────────────────
app.include_router(auth.router,       prefix="/api/v1")
app.include_router(agents.router,     prefix="/api/v1")
app.include_router(tickets.router,    prefix="/api/v1")
app.include_router(websockets.router)   # no prefix — WS paths are /ws/...


@app.get("/", tags=["Health"])
async def health():
    return {"status": "ok", "service": "CNAS Queue API v2"}
