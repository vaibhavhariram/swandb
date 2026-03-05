"""SQLAlchemy async engine and session."""

from collections.abc import AsyncGenerator
from typing import Optional

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

# Use asyncpg for async; URL format: postgresql+asyncpg://
# For sync (Alembic), env.py uses postgresql:// with psycopg


class Base(DeclarativeBase):
    """Declarative base for all models."""

    pass


_engine = None
_session_factory: Optional[async_sessionmaker[AsyncSession]] = None


def init_async_engine(database_url: str) -> None:
    """Initialize async engine. Call at app startup."""
    global _engine, _session_factory
    if database_url.startswith("postgresql://") and "+asyncpg" not in database_url:
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    _engine = create_async_engine(
        database_url,
        echo=False,
        pool_pre_ping=True,
    )
    _session_factory = async_sessionmaker(
        _engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )


def get_async_session() -> async_sessionmaker[AsyncSession]:
    """Return the async session factory. Raises if init_async_engine not called."""
    if _session_factory is None:
        raise RuntimeError("init_async_engine must be called before get_async_session")
    return _session_factory


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Dependency: yield an async session."""
    factory = get_async_session()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
