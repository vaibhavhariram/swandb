"""Online feature serving API."""

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends

from swandb.online.store import get_features_async
from services.api.auth import AuthContext, require_api_key
from services.api.config import settings
from services.api.schemas import (
    FeatureValue,
    GetFeaturesRequest,
    GetFeaturesResponse,
)

router = APIRouter(prefix="/features", tags=["features"])


@router.post("/get", response_model=GetFeaturesResponse)
async def post_get_features(
    request: GetFeaturesRequest,
    auth: AuthContext = Depends(require_api_key),
) -> GetFeaturesResponse:
    """
    Fetch online features for an entity at a point in time.
    Uses bucketed as-of key; falls back to current if out-of-window.
    """
    client = aioredis.from_url(settings.redis_url)
    try:
        feature_refs = [{"name": r.name, "version": r.version} for r in request.feature_refs]
        results = await get_features_async(
            client,
            str(auth.tenant_id),
            request.entity_keys,
            feature_refs,
            request.as_of_ts,
            entity_key="user_id",
        )
        return GetFeaturesResponse(
            features=[
                FeatureValue(
                    feature_ref=r["feature_ref"],
                    value=r["value"],
                    feature_ts=r["feature_ts"],
                    status=r["status"],
                )
                for r in results
            ]
        )
    finally:
        await client.aclose()
