"""API key auth dependency for tenant-scoped routes."""

from typing import Annotated

from fastapi import Header, HTTPException, Path

from swandb.auth import verify_api_key
from swandb.db.base import get_async_session


class AuthContext:
    """Request auth context: tenant_id and scopes from validated API key."""

    def __init__(self, tenant_id: str, scopes: list[str]) -> None:
        self.tenant_id = tenant_id
        self.scopes = scopes


async def require_api_key(
    tenant_id: Annotated[str, Path(description="Tenant ID from path")],
    authorization: str | None = Header(default=None),
) -> AuthContext:
    """
    Validate API key and ensure path tenant matches key tenant.
    Raises 401 if missing or invalid key.
    """
    plain_key = None
    if authorization and authorization.startswith("Bearer "):
        plain_key = authorization[7:].strip()

    if not plain_key:
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")

    session_factory = get_async_session()
    async with session_factory() as session:
        result = await verify_api_key(session, plain_key)
        await session.commit()

    if result is None:
        raise HTTPException(status_code=401, detail="Invalid API key")

    key_tenant_id, scopes = result
    if key_tenant_id != tenant_id:
        raise HTTPException(status_code=401, detail="Tenant mismatch")

    return AuthContext(tenant_id=key_tenant_id, scopes=scopes)
