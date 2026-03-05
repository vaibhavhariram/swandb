"""Training build API."""

from pathlib import Path

from chronosdb.registry.store import get_feature, get_feature_version
from chronosdb.training.build import build_training_dataset
from fastapi import APIRouter, Depends, HTTPException
from services.api.auth import AuthContext, require_api_key
from services.api.config import settings
from services.api.schemas import TrainingBuildRequest, TrainingBuildResponse
from sqlalchemy.ext.asyncio import AsyncSession

from chronosdb.db.base import get_session

router = APIRouter(prefix="/training", tags=["training"])


@router.post("/build", response_model=TrainingBuildResponse)
async def post_training_build(
    request: TrainingBuildRequest,
    auth: AuthContext = Depends(require_api_key),
    session: AsyncSession = Depends(get_session),
) -> TrainingBuildResponse:
    """
    Build training dataset via PIT-correct ASOF JOIN.
    Reads labels parquet, joins with feature parquet, outputs training parquet + manifest.
    """
    base_path = Path(settings.offline_objects_path)
    labels_path = Path(request.labels_path)
    if not labels_path.is_absolute():
        labels_path = base_path / labels_path

    feature_refs = [{"name": r.name, "version": r.version} for r in request.feature_refs]
    feature_specs = []
    for ref in feature_refs:
        feature = await get_feature(session, auth.tenant_id, ref["name"])
        if not feature:
            raise HTTPException(status_code=404, detail=f"Feature not found: {ref['name']}")
        fv = await get_feature_version(session, feature.id, ref["version"])
        if not fv:
            raise HTTPException(status_code=404, detail=f"Feature version not found: {ref['name']} v{ref['version']}")
        feature_specs.append({"spec_hash": fv.spec_hash})

    out_path, manifest_path, manifest = build_training_dataset(
        labels_path=labels_path,
        base_path=base_path,
        tenant_id=auth.tenant_id,
        feature_refs=feature_refs,
        feature_specs=feature_specs,
        entity_key=request.entity_key,
    )

    return TrainingBuildResponse(
        build_id=manifest["build_id"],
        output_path=str(out_path),
        manifest_path=str(manifest_path),
        row_count=manifest["row_count"],
        feature_refs=manifest["feature_refs"],
        spec_hashes=manifest["spec_hashes"],
    )
