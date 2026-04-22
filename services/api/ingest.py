"""Event ingestion API routes."""

from swandb.db.base import get_session
from swandb.ingest.service import ingest_events
from fastapi import APIRouter, Depends, HTTPException
from services.api.auth import AuthContext, require_api_key
from services.api.config import settings
from services.api.schemas import IngestEventsRequest, IngestEventsResponse
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/ingest", tags=["ingest"])


@router.post("/events", response_model=IngestEventsResponse)
async def post_ingest_events(
    request: IngestEventsRequest,
    auth: AuthContext = Depends(require_api_key),
    session: AsyncSession = Depends(get_session),
) -> IngestEventsResponse:
    """
    Ingest a batch of events. Idempotent: same idempotency_key returns prior job.
    """
    events_data = [e.model_dump() for e in request.events]
    job_id, event_count, max_event_ts, max_ingest_ts = await ingest_events(
        session=session,
        base_path=settings.offline_objects_path,
        tenant_id=auth.tenant_id,
        source_id=request.source_id,
        idempotency_key=request.idempotency_key,
        events=events_data,
    )
    return IngestEventsResponse(
        job_id=job_id,
        event_count=event_count,
        max_event_ts=max_event_ts,
        max_ingest_ts=max_ingest_ts,
    )
