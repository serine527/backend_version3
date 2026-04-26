from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+asyncpg://postgres:password@localhost:5432/cnas_db"
    REDIS_URL: str = "redis://localhost:6379"
    SECRET_KEY: str = "change-this"
    APP_ENV: str = "development"

    class Config:
        env_file = ".env"

settings = Settings()
