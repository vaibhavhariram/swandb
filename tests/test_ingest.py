"""Tests for event ingestion."""

import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pyarrow.parquet as pq
import pytest

from swandb.offline.events import dedupe_events
from swandb.offline.layout import events_path
from swandb.offline.writer import EVENTS_SCHEMA, write_events_parquet


def test_write_events_parquet_schema() -> None:
    """Ingest writes a parquet file with expected schema."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "events.parquet"
        events = [
            {
                "event_id": "e1",
                "event_hash": "abc123",
                "entity_keys": {"user_id": "u1"},
                "event_ts": datetime(2025, 3, 5, 12, 0, 0, tzinfo=timezone.utc),
                "event_type": "click",
                "payload": {"page": "/home"},
                "ingest_ts": datetime.now(timezone.utc),
            }
        ]
        written = write_events_parquet(path, events)
        assert written == 1
        assert path.exists()
        table = pq.read_table(path)
        assert table.column_names == list(EVENTS_SCHEMA.names)
        assert len(table) == 1


def test_ingest_writes_parquet_via_service() -> None:
    """Ingest service writes parquet when no existing idempotent job."""
    from swandb.ingest.service import ingest_events

    async def _run():
        result = MagicMock()
        result.scalar_one_or_none = lambda: None  # No existing job
        mock_session = MagicMock()
        mock_session.execute = AsyncMock(return_value=result)
        mock_session.add = MagicMock()
        mock_session.flush = AsyncMock()

        with tempfile.TemporaryDirectory() as tmp:
            events_data = [
                {
                    "entity_keys": {"user_id": "u1"},
                    "event_ts": datetime(2025, 3, 5, 12, 0, 0, tzinfo=timezone.utc),
                    "event_type": "click",
                    "payload": {},
                }
            ]
            job_id, count, max_et, max_it = await ingest_events(
                session=mock_session,
                base_path=tmp,
                tenant_id="11111111-1111-1111-1111-111111111111",
                source_id="22222222-2222-2222-2222-222222222222",
                idempotency_key="idem-new",
                events=events_data,
            )
            path = Path(tmp) / "events" / "tenant=11111111-1111-1111-1111-111111111111" / "source=22222222-2222-2222-2222-222222222222" / "dt=2025-03-05" / "part-0000.parquet"
            return job_id, count, path.exists()

    import asyncio
    job_id, count, path_exists = asyncio.run(_run())

    assert job_id
    assert count == 1
    assert path_exists


def test_ingest_idempotency_returns_prior_job() -> None:
    """When get_existing_idempotent_job returns a job, ingest_events returns it without writing."""
    from swandb.ingest.service import ingest_events

    async def _run():
        mock_session = MagicMock()
        existing_job = MagicMock()
        existing_job.id = "prior-job-456"
        existing_job.event_count = 5
        existing_job.max_event_ts = datetime(2025, 3, 5, 12, 0, 0, tzinfo=timezone.utc)
        existing_job.max_ingest_ts = datetime(2025, 3, 5, 12, 1, 0, tzinfo=timezone.utc)

        result = MagicMock()
        result.scalar_one_or_none = lambda: existing_job
        mock_session.execute = AsyncMock(return_value=result)

        with tempfile.TemporaryDirectory() as tmp:
            job_id, count, max_et, max_it = await ingest_events(
                session=mock_session,
                base_path=tmp,
                tenant_id="11111111-1111-1111-1111-111111111111",
                source_id="22222222-2222-2222-2222-222222222222",
                idempotency_key="idem-prior",
                events=[{"entity_keys": {}, "event_ts": "2025-03-05T12:00:00Z", "event_type": "x", "payload": {}}],
            )
        return job_id, count, max_et, max_it

    import asyncio
    job_id, count, max_et, max_it = asyncio.run(_run())

    assert job_id == "prior-job-456"
    assert count == 5
    assert max_et is not None
    assert max_it is not None


def test_events_path_layout() -> None:
    """Layout produces correct path."""
    from datetime import date
    p = events_path("/offline", "t1", "s1", date(2025, 3, 5))
    assert "tenant=t1" in str(p)
    assert "source=s1" in str(p)
    assert "dt=2025-03-05" in str(p)
    assert p.name == "part-0000.parquet"


def test_dedupe_prefers_event_id() -> None:
    """Dedupe uses event_id when provided."""
    events = [
        {"event_id": "e1", "event_hash": "h1", "entity_keys": {}, "event_ts": None, "event_type": "", "payload": {}, "ingest_ts": None},
        {"event_id": "e1", "event_hash": "h2", "entity_keys": {}, "event_ts": None, "event_type": "", "payload": {}, "ingest_ts": None},
    ]
    deduped = dedupe_events(events)
    assert len(deduped) == 1
    assert deduped[0]["event_id"] == "e1"


def test_dedupe_uses_event_hash_when_no_event_id() -> None:
    """Dedupe uses event_hash when event_id not provided."""
    events = [
        {"event_id": "", "event_hash": "h1", "entity_keys": {}, "event_ts": None, "event_type": "", "payload": {}, "ingest_ts": None},
        {"event_id": "", "event_hash": "h1", "entity_keys": {}, "event_ts": None, "event_type": "", "payload": {}, "ingest_ts": None},
    ]
    deduped = dedupe_events(events)
    assert len(deduped) == 1
