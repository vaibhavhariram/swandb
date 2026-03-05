"""Parity validation API."""

from chronosdb.db.models import QualityResult
from chronosdb.registry.store import get_feature, get_feature_version
from chronosdb.validate.parity import run_parity_validation
from fastapi import APIRouter, Depends, HTTPException
from services.api.auth import AuthContext, require_api_key
from services.api.config import settings
from services.api.schemas import ValidateParityRequest, ValidateParityResponse
from sqlalchemy.ext.asyncio import AsyncSession

from chronosdb.db.base import get_session

router = APIRouter(prefix="/validate", tags=["validate"])


async def _resolve_entity_keys(
    session: AsyncSession,
    tenant_id: str,
    feature_refs: list[dict],
    request_entity_keys: list[str] | None,
) -> list[str]:
    """
    Resolve entity_keys from registry (features.entity_keys).
    All feature_refs must match the same entity_keys list.
    If request provides entity_keys, it must match registry exactly; else 400.
    """
    if not feature_refs:
        return ["user_id"]
    first = feature_refs[0]
    feature = await get_feature(session, tenant_id, first["name"])
    if not feature:
        raise HTTPException(status_code=404, detail=f"Feature not found: {first['name']}")
    await get_feature_version(session, feature.id, first.get("version", 1))
    registry_keys = list(feature.entity_keys) if feature.entity_keys else ["user_id"]

    for ref in feature_refs[1:]:
        f = await get_feature(session, tenant_id, ref["name"])
        if not f:
            raise HTTPException(status_code=404, detail=f"Feature not found: {ref['name']}")
        await get_feature_version(session, f.id, ref.get("version", 1))
        ref_keys = list(f.entity_keys) if f.entity_keys else ["user_id"]
        if ref_keys != registry_keys:
            raise HTTPException(
                status_code=400,
                detail=f"All feature_refs must use same entity_keys; {first['name']} has {registry_keys}, {ref['name']} has {ref_keys}",
            )

    if request_entity_keys is not None:
        if list(request_entity_keys) != registry_keys:
            raise HTTPException(
                status_code=400,
                detail=f"Request entity_keys {request_entity_keys} does not match registry {registry_keys}",
            )
        return list(request_entity_keys)
    return registry_keys


@router.post("/parity", response_model=ValidateParityResponse)
async def post_validate_parity(
    request: ValidateParityRequest,
    auth: AuthContext = Depends(require_api_key),
    session: AsyncSession = Depends(get_session),
) -> ValidateParityResponse:
    """
    Validate offline/online parity. Sample N entities, compare offline vs online bucketed fetch.
    Fails with 400 if mismatch_rate > threshold.
    """
    base_path = settings.offline_objects_path
    feature_refs = [{"name": r.name, "version": r.version} for r in request.feature_refs]

    entity_keys = await _resolve_entity_keys(
        session, auth.tenant_id, feature_refs, request.entity_keys
    )

    result = await run_parity_validation(
        base_path=base_path,
        tenant_id=auth.tenant_id,
        feature_refs=feature_refs,
        entity_keys=entity_keys,
        redis_url=settings.redis_url,
        sample_size=request.sample_size,
        threshold=request.threshold,
    )

    qr = QualityResult(
        tenant_id=auth.tenant_id,
        job_type="parity",
        status=result["status"],
        sample_size=result.get("sample_size"),
        mismatch_count=result.get("mismatch_count"),
        mismatch_rate=result.get("mismatch_rate"),
        threshold=result.get("threshold"),
        details=result.get("details"),
    )
    session.add(qr)
    await session.flush()

    if result["status"] == "failed":
        raise HTTPException(
            status_code=400,
            detail=f"Parity validation failed: mismatch_rate={result['mismatch_rate']:.4f} > threshold={request.threshold}",
        )

    return ValidateParityResponse(
        job_id=str(qr.id),
        status=result["status"],
        sample_size=result.get("sample_size", 0),
        mismatch_count=result.get("mismatch_count", 0),
        mismatch_rate=result.get("mismatch_rate", 0.0),
        threshold=request.threshold,
    )
