"""Registry store: tenants, API keys, sources."""

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from chronosdb.auth import hash_key
from chronosdb.db.models import ApiKey, Source, Tenant


async def create_tenant(session: AsyncSession, name: str) -> Tenant:
    """Create a tenant. Raises if name already exists."""
    tenant = Tenant(name=name)
    session.add(tenant)
    await session.flush()
    return tenant


async def create_api_key(
    session: AsyncSession,
    tenant_id: str | uuid.UUID,
    plain_key: str,
    scopes: list[str] | None = None,
) -> ApiKey:
    """Create an API key. Store hashed key. Returns the ApiKey row."""
    tid = uuid.UUID(str(tenant_id)) if isinstance(tenant_id, str) else tenant_id
    key_hash = hash_key(plain_key)
    api_key = ApiKey(
        tenant_id=tid,
        key_hash=key_hash,
        scopes=scopes or [],
    )
    session.add(api_key)
    await session.flush()
    return api_key


async def upsert_source(
    session: AsyncSession,
    tenant_id: str | uuid.UUID,
    name: str,
    type: str,
    config: dict[str, Any] | None = None,
) -> Source:
    """Insert or update a source. Unique on (tenant_id, name)."""
    tid = uuid.UUID(str(tenant_id)) if isinstance(tenant_id, str) else tenant_id
    result = await session.execute(
        select(Source).where(
            Source.tenant_id == tid,
            Source.name == name,
        )
    )
    existing = result.scalar_one_or_none()
    if existing:
        existing.type = type
        existing.config = config or {}
        await session.flush()
        return existing
    source = Source(
        tenant_id=tid,
        name=name,
        type=type,
        config=config or {},
    )
    session.add(source)
    await session.flush()
    return source
