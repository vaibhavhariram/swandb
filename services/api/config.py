"""API configuration from environment variables."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgresql://chronosdb:chronosdb@localhost:5432/chronosdb"
    redis_url: str = "redis://localhost:6379/0"


settings = Settings()
