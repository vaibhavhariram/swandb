"""API key hashing and verification."""

import hashlib
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from swandb.db.models import ApiKey

# Prefix for stored hashes (allows future algorithm rotation)
HASH_PREFIX = "sha256:"


def hash_key(plain_key: str) -> str:
    """Hash an API key for storage. Uses SHA-256."""
    digest = hashlib.sha256(plain_key.encode("utf-8")).hexdigest()
    return f"{HASH_PREFIX}{digest}"


async def verify_api_key(
    session: AsyncSession,
    plain_key: str,
) -> Optional[tuple[str, list[str]]]:
    """
    Verify API key and return (tenant_id_str, scopes) or None.
    """
    key_hash = hash_key(plain_key)
    result = await session.execute(
        select(ApiKey).where(ApiKey.key_hash == key_hash)
    )
    row = result.scalar_one_or_none()
    if row is None:
        return None
    return (str(row.tenant_id), list(row.scopes or []))
