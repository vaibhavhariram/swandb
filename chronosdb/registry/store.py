"""Registry store: tenants, API keys, sources, features."""

import uuid
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from chronosdb.auth import hash_key
from chronosdb.db.models import ApiKey, Feature, FeatureVersion, Source, Tenant
from chronosdb.transforms.spec import compute_spec_hash


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


async def create_feature(
    session: AsyncSession,
    tenant_id: str | uuid.UUID,
    name: str,
    source_id: str | uuid.UUID | None = None,
) -> Feature:
    """Create a feature. Raises if (tenant_id, name) already exists."""
    tid = uuid.UUID(str(tenant_id)) if isinstance(tenant_id, str) else tenant_id
    sid = uuid.UUID(str(source_id)) if source_id is not None and isinstance(source_id, str) else source_id
    feature = Feature(tenant_id=tid, name=name, source_id=sid)
    session.add(feature)
    await session.flush()
    return feature


async def get_feature(
    session: AsyncSession,
    tenant_id: str | uuid.UUID,
    name: str,
) -> Feature | None:
    """Get feature by tenant and name."""
    tid = uuid.UUID(str(tenant_id)) if isinstance(tenant_id, str) else tenant_id
    result = await session.execute(
        select(Feature).where(
            Feature.tenant_id == tid,
            Feature.name == name,
        )
    )
    return result.scalar_one_or_none()


async def create_feature_version(
    session: AsyncSession,
    feature_id: str | uuid.UUID,
    transform_spec: dict[str, Any],
) -> FeatureVersion:
    """
    Create a new feature version. Enforces immutability: versions cannot be overwritten.
    Computes spec_hash from transform_spec.
    """
    fid = uuid.UUID(str(feature_id)) if isinstance(feature_id, str) else feature_id
    spec_hash = compute_spec_hash(transform_spec)

    # Get next version number
    subq = (
        select(func.coalesce(func.max(FeatureVersion.version), 0) + 1)
        .where(FeatureVersion.feature_id == fid)
    )
    result = await session.execute(subq)
    next_version = result.scalar_one()

    fv = FeatureVersion(
        feature_id=fid,
        version=next_version,
        transform_spec=transform_spec,
        spec_hash=spec_hash,
    )
    session.add(fv)
    await session.flush()
    return fv


async def get_feature_versions(
    session: AsyncSession,
    feature_id: str | uuid.UUID,
) -> list[FeatureVersion]:
    """Get all versions of a feature, ordered by version ascending."""
    fid = uuid.UUID(str(feature_id)) if isinstance(feature_id, str) else feature_id
    result = await session.execute(
        select(FeatureVersion)
        .where(FeatureVersion.feature_id == fid)
        .order_by(FeatureVersion.version)
    )
    return list(result.scalars().all())
