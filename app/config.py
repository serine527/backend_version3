#app\config.py
from pydantic_settings import BaseSettings
from typing import Literal


class Settings(BaseSettings):
    # ── Core ─────────────────────────────────────────────
    APP_ENV: str = "development"
    SECRET_KEY: str = "change-this"

    # ── Database ─────────────────────────────────────────
    DATABASE_URL: str = "postgresql+asyncpg://postgres:password@localhost:5432/cnas_db"

    # ── Redis ────────────────────────────────────────────
    REDIS_URL: str = "redis://localhost:6379"

    # ── Queue System Config ──────────────────────────────
    # Mode:
    # - "unique" → guichet unique (ignore sub_service)
    # - "multi"  → multi-guichet (use sub_service)
    QUEUE_MODE: Literal["single", "multi"] = "multi"

    # Optional: allow fallback if no exact sub_service match
    ALLOW_SUBSERVICE_FALLBACK: bool = True

    # Optional: max tickets per agent (for load protection)
    MAX_QUEUE_PER_AGENT: int = 50


    class Config:
        env_file = ".env"


settings = Settings()