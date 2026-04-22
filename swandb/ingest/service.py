"""Event ingestion service: dedupe, write parquet, record job."""

import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from swandb.db.models import IngestionJob
from swandb.offline.events import compute_event_hash, dedupe_events
from swandb.offline.layout import events_path
from swandb.offline.writer import write_events_parquet


def _to_datetime(ts: datetime | str) -> datetime:
    """Ensure datetime, parse ISO string if needed."""
    if isinstance(ts, datetime):
        return ts
    return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))


async def get_existing_idempotent_job(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    source_id: uuid.UUID,
    idempotency_key: str,
) -> IngestionJob | None:
    """Return existing succeeded job for idempotency_key, or None."""
    result = await session.execute(
        select(IngestionJob)
        .where(
            IngestionJob.tenant_id == tenant_id,
            IngestionJob.source_id == source_id,
            IngestionJob.idempotency_key == idempotency_key,
            IngestionJob.status == "succeeded",
        )
        .order_by(IngestionJob.created_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def ingest_events(
    session: AsyncSession,
    base_path: str | Path,
    tenant_id: str,
    source_id: str,
    idempotency_key: str,
    events: list[dict[str, Any]],
) -> tuple[str, int, datetime | None, datetime | None]:
    """
    Ingest events. Returns (job_id, event_count, max_event_ts, max_ingest_ts).
    If idempotent replay: returns existing job_id and counts without writing.
    """
    tid = uuid.UUID(tenant_id)
    sid = uuid.UUID(source_id)

    # Idempotency check
    existing = await get_existing_idempotent_job(session, tid, sid, idempotency_key)
    if existing:
        return (
            str(existing.id),
            existing.event_count or 0,
            existing.max_event_ts,
            existing.max_ingest_ts,
        )

    # Prepare events: compute event_hash, add ingest_ts
    ingest_ts = datetime.now(timezone.utc)
    prepared: list[dict[str, Any]] = []
    for e in events:
        entity_keys = e.get("entity_keys") or {}
        event_ts = _to_datetime(e["event_ts"])
        event_type = e.get("event_type") or ""
        payload = e.get("payload") or {}
        event_hash = (
            str(e["event_id"]) if e.get("event_id") else
            compute_event_hash(entity_keys, event_ts, event_type, payload)
        )
        prepared.append({
            "event_id": str(e.get("event_id") or ""),
            "event_hash": event_hash,
            "entity_keys": entity_keys,
            "event_ts": event_ts,
            "event_type": event_type,
            "payload": payload,
            "ingest_ts": ingest_ts,
        })

    # Dedupe
    deduped = dedupe_events(prepared)
    if not deduped:
        job = IngestionJob(
            tenant_id=tid,
            source_id=sid,
            status="succeeded",
            idempotency_key=idempotency_key,
            event_count=0,
            max_event_ts=None,
            max_ingest_ts=ingest_ts,
        )
        session.add(job)
        await session.flush()
        return (str(job.id), 0, None, ingest_ts)

    # Group by date, write one parquet per partition
    by_date: dict[Any, list] = defaultdict(list)
    for ev in deduped:
        dt = ev["event_ts"].date() if hasattr(ev["event_ts"], "date") else _to_datetime(ev["event_ts"]).date()
        by_date[dt].append(ev)

    written = 0
    max_event_ts = None
    for dt, batch in by_date.items():
        path = events_path(base_path, tenant_id, source_id, dt)
        written += write_events_parquet(path, batch)
        for ev in batch:
            ts = ev["event_ts"]
            if max_event_ts is None or ts > max_event_ts:
                max_event_ts = ts

    job = IngestionJob(
        tenant_id=tid,
        source_id=sid,
        status="succeeded",
        idempotency_key=idempotency_key,
        event_count=written,
        max_event_ts=max_event_ts,
        max_ingest_ts=ingest_ts,
    )
    session.add(job)
    await session.flush()

    return (str(job.id), written, max_event_ts, ingest_ts)
