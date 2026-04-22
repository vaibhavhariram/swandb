"""Feature registry API routes."""

from sqlalchemy.exc import IntegrityError

from swandb.db.base import get_session
from swandb.registry.store import (
    create_feature,
    create_feature_version,
    get_feature,
    get_feature_versions,
)
from fastapi import APIRouter, Depends, HTTPException
from services.api.auth import AuthContext, require_api_key
from services.api.schemas import (
    CreateFeatureRequest,
    CreateFeatureResponse,
    CreateFeatureVersionRequest,
    FeatureVersionResponse,
    GetFeatureResponse,
)
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/registry", tags=["registry"])


@router.post("/features", response_model=CreateFeatureResponse)
async def post_feature(
    request: CreateFeatureRequest,
    auth: AuthContext = Depends(require_api_key),
    session: AsyncSession = Depends(get_session),
) -> CreateFeatureResponse:
    """Create a feature. Tenant from auth context."""
    try:
        feature = await create_feature(
            session,
            tenant_id=auth.tenant_id,
            name=request.name,
            source_id=request.source_id,
        )
    except IntegrityError:
        raise HTTPException(status_code=409, detail="Feature already exists")
    return CreateFeatureResponse(
        id=str(feature.id),
        name=feature.name,
        tenant_id=str(feature.tenant_id),
        source_id=str(feature.source_id) if feature.source_id else None,
        created_at=feature.created_at,
    )


@router.post("/features/{name}/versions", response_model=FeatureVersionResponse)
async def post_feature_version(
    name: str,
    request: CreateFeatureVersionRequest,
    auth: AuthContext = Depends(require_api_key),
    session: AsyncSession = Depends(get_session),
) -> FeatureVersionResponse:
    """Create a new version for a feature. Immutable per version."""
    feature = await get_feature(session, tenant_id=auth.tenant_id, name=name)
    if feature is None:
        raise HTTPException(status_code=404, detail="Feature not found")

    fv = await create_feature_version(
        session,
        feature_id=feature.id,
        transform_spec=request.transform_spec,
    )
    return FeatureVersionResponse(
        id=str(fv.id),
        version=fv.version,
        transform_spec=fv.transform_spec,
        spec_hash=fv.spec_hash,
        created_at=fv.created_at,
    )


@router.get("/features/{name}", response_model=GetFeatureResponse)
async def get_feature_endpoint(
    name: str,
    auth: AuthContext = Depends(require_api_key),
    session: AsyncSession = Depends(get_session),
) -> GetFeatureResponse:
    """Get a feature with all its versions."""
    feature = await get_feature(session, tenant_id=auth.tenant_id, name=name)
    if feature is None:
        raise HTTPException(status_code=404, detail="Feature not found")

    versions = await get_feature_versions(session, feature_id=feature.id)
    return GetFeatureResponse(
        id=str(feature.id),
        name=feature.name,
        tenant_id=str(feature.tenant_id),
        source_id=str(feature.source_id) if feature.source_id else None,
        created_at=feature.created_at,
        versions=[
            FeatureVersionResponse(
                id=str(v.id),
                version=v.version,
                transform_spec=v.transform_spec,
                spec_hash=v.spec_hash,
                created_at=v.created_at,
            )
            for v in versions
        ],
    )
