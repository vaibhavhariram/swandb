"""Alembic environment. Uses sync SQLAlchemy with psycopg for migrations."""

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import Connection

# Import models so Alembic can see them for autogenerate
from chronosdb.db.base import Base
from chronosdb.db.models import (  # noqa: F401
    ApiKey,
    Checkpoint,
    Feature,
    FeatureVersion,
    IngestionJob,
    MaterializationJob,
    Source,
    Tenant,
)

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_url() -> str:
    """Get database URL from DATABASE_URL env. Use sync driver for Alembic."""
    url = os.environ.get(
        "DATABASE_URL",
        "postgresql://chronosdb:chronosdb@localhost:5432/chronosdb",
    )
    # Alembic uses sync; ensure we use postgresql:// (psycopg), not postgresql+asyncpg
    if "+asyncpg" in url:
        url = url.replace("postgresql+asyncpg://", "postgresql://", 1)
    return url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = get_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
