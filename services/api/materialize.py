"""Materialization API: enqueue offline feature materialization jobs."""

from chronosdb.db.base import get_session
from chronosdb.db.models import MaterializationJob
from fastapi import APIRouter, Depends, HTTPException
from services.api.auth import AuthContext, require_api_key
from services.api.config import settings
from services.api.schemas import MaterializeRequest, MaterializeResponse
from services.worker.queue import materialize_enqueue
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/materialize", tags=["materialize"])


@router.post("", response_model=MaterializeResponse)
async def post_materialize(
    request: MaterializeRequest,
    auth: AuthContext = Depends(require_api_key),
    session: AsyncSession = Depends(get_session),
) -> MaterializeResponse:
    """
    Enqueue a materialization job. Returns job_id immediately.
    Job runs in background (in-process worker for MVP).
    """
    if request.range_end < request.range_start:
        raise HTTPException(
            status_code=400,
            detail="range_end must be >= range_start",
        )

    feature_refs = [{"name": r.name, "version": r.version} for r in request.feature_refs]

    job = MaterializationJob(
        tenant_id=auth.tenant_id,
        status="pending",
        range_start=request.range_start,
        range_end=request.range_end,
        feature_refs=feature_refs,
    )
    session.add(job)
    await session.flush()

    materialize_enqueue({
        "job_id": str(job.id),
        "tenant_id": str(auth.tenant_id),
        "range_start": request.range_start,
        "range_end": request.range_end,
        "feature_refs": feature_refs,
        "base_path": settings.offline_objects_path,
        "database_url": settings.database_url,
        "redis_url": settings.redis_url,
    })

    return MaterializeResponse(job_id=str(job.id))
