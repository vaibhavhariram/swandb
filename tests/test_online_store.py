"""Tests for online feature store (Redis)."""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from chronosdb.online.key_format import (
    bucket_key,
    bucket_ts_from_as_of,
    current_key,
)
from chronosdb.online.store import (
    extract_rows_for_redis,
    get_features_async,
    write_features_sync,
)


def test_current_key_format() -> None:
    """Current key has expected format."""
    k = current_key("t1", "amount", 1, "u1")
    assert k == "chronosdb:t1:amount:v1:current:u1"


def test_bucket_key_format() -> None:
    """Bucket key has expected format."""
    k = bucket_key("t1", "amount", 1, "2025-03-05T10:37:00", "u1")
    assert k == "chronosdb:t1:amount:v1:b:2025-03-05T10:37:00:u1"


def test_bucket_ts_from_as_of() -> None:
    """Bucket floors to minute."""
    assert bucket_ts_from_as_of("2025-03-05T10:37:22Z") == "2025-03-05T10:37:00"
    assert bucket_ts_from_as_of(datetime(2025, 3, 5, 10, 37, 59, tzinfo=timezone.utc)) == "2025-03-05T10:37:00"


def test_extract_rows_for_redis_last_value() -> None:
    """Extract rows from last_value schema (entity_id, as_of_event_ts)."""
    import pyarrow as pa
    table = pa.table({
        "entity_id": ["u1", "u2"],
        "value": ["100", "50"],
        "as_of_event_ts": [
            datetime(2025, 3, 5, 10, 0, 0, tzinfo=timezone.utc),
            datetime(2025, 3, 5, 10, 30, 0, tzinfo=timezone.utc),
        ],
    })
    rows = extract_rows_for_redis(table, {"entity_key": "user_id"})
    assert len(rows) == 2
    assert rows[0] == ("u1", "100", "2025-03-05T10:00:00+00:00")
    assert rows[1] == ("u2", "50", "2025-03-05T10:30:00+00:00")


def test_extract_rows_for_redis_passthrough() -> None:
    """Extract rows from passthrough schema (entity_keys, event_ts)."""
    import pyarrow as pa
    table = pa.table({
        "entity_keys": ['{"user_id": "u1"}', '{"user_id": "u2"}'],
        "value": ["100", "50"],
        "event_ts": [
            datetime(2025, 3, 5, 10, 0, 0, tzinfo=timezone.utc),
            datetime(2025, 3, 5, 10, 30, 0, tzinfo=timezone.utc),
        ],
    })
    rows = extract_rows_for_redis(table, {"entity_key": "user_id"})
    assert len(rows) == 2
    assert rows[0] == ("u1", "100", "2025-03-05T10:00:00+00:00")
    assert rows[1] == ("u2", "50", "2025-03-05T10:30:00+00:00")


def test_write_features_sync_writes_redis() -> None:
    """Materialize write path: write_features_sync writes current + bucket keys."""
    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    mock_redis.pipeline.return_value = mock_pipe

    write_features_sync(
        mock_redis,
        "t1",
        "amount",
        1,
        [("u1", "100", "2025-03-05T10:37:00"), ("u2", "50", "2025-03-05T10:37:00")],
        ttl_seconds=3600,
    )

    assert mock_pipe.set.call_count == 4  # 2 rows * 2 keys each
    set_calls = mock_pipe.set.call_args_list
    keys = [c[0][0] for c in set_calls]
    assert any("current" in k for k in keys)
    assert any("b:2025-03-05T10:37:00" in k for k in keys)
    mock_pipe.execute.assert_called_once()


@pytest.mark.asyncio
async def test_get_features_returns_values_and_feature_ts() -> None:
    """/features/get returns expected values and feature_ts."""
    mock_redis = AsyncMock()
    payload = json.dumps({"value": "100", "feature_ts": "2025-03-05T10:37:00"})
    mock_redis.mget = AsyncMock(side_effect=[
        [payload.encode()],   # bucket keys
        [payload.encode()],   # current keys (fallback)
    ])

    results = await get_features_async(
        mock_redis,
        "t1",
        {"user_id": "u1"},
        [{"name": "amount", "version": 1}],
        "2025-03-05T10:37:00",
    )

    assert len(results) == 1
    assert results[0]["value"] == "100"
    assert results[0]["feature_ts"] == "2025-03-05T10:37:00"
    assert results[0]["status"] == "ok"


@pytest.mark.asyncio
async def test_get_features_fallback_to_current() -> None:
    """When bucket key missing, fall back to current."""
    mock_redis = AsyncMock()
    payload = json.dumps({"value": "100", "feature_ts": "2025-03-05T10:37:00"})
    mock_redis.mget = AsyncMock(side_effect=[
        [None],               # bucket key not found
        [payload.encode()],   # current key found
    ])

    results = await get_features_async(
        mock_redis,
        "t1",
        {"user_id": "u1"},
        [{"name": "amount", "version": 1}],
        "2025-03-05T10:37:00",
    )

    assert len(results) == 1
    assert results[0]["value"] == "100"
    assert results[0]["status"] == "current_fallback"


@pytest.mark.integration
def test_materialize_writes_redis_keys() -> None:
    """Materialize job invokes write_features_sync when redis_url provided. Requires RUN_INTEGRATION=1."""
    import os
    if os.environ.get("RUN_INTEGRATION") != "1":
        pytest.skip("Set RUN_INTEGRATION=1 to run")

    from unittest.mock import patch
    import tempfile
    from pathlib import Path
    from datetime import date

    db_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://chronosdb:chronosdb@localhost:5432/chronosdb",
    )
    if "postgresql" not in db_url:
        pytest.skip("Requires Postgres (DATABASE_URL)")

    sync_url = db_url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
    if sync_url.startswith("postgresql://") and "+" not in sync_url:
        sync_url = sync_url.replace("postgresql://", "postgresql+psycopg://", 1)

    with patch("services.worker.jobs.materialize.online_store.write_features_sync") as mock_write:
        mock_write.return_value = 2
        with tempfile.TemporaryDirectory() as tmp:
            base_path = Path(tmp)
            from chronosdb.offline.writer import write_events_parquet
            from chronosdb.offline.layout import events_path
            from sqlalchemy import create_engine
            from sqlalchemy.orm import sessionmaker
            from chronosdb.db.base import Base
            from chronosdb.db.models import Tenant, Source, Feature, FeatureVersion

            engine = create_engine(sync_url)
            Base.metadata.create_all(engine)
            Session = sessionmaker(engine, expire_on_commit=False)
            session = Session()
            tenant = Tenant(name=f"t-{__import__('uuid').uuid4().hex[:8]}")
            session.add(tenant)
            session.flush()
            source = Source(tenant_id=tenant.id, name="s", type="file", config={})
            session.add(source)
            session.flush()
            feature = Feature(tenant_id=tenant.id, name="amount", source_id=source.id)
            session.add(feature)
            session.flush()
            fv = FeatureVersion(
                feature_id=feature.id, version=1,
                transform_spec={"type": "passthrough", "field": "amount"},
                spec_hash="h",
            )
            session.add(fv)
            session.commit()

            events = [{
                "event_id": "e1", "event_hash": "h1",
                "entity_keys": {"user_id": "u1"},
                "event_ts": datetime(2025, 3, 5, 10, 0, 0, tzinfo=timezone.utc),
                "event_type": "click", "payload": {"amount": 100},
                "ingest_ts": datetime(2025, 3, 5, 10, 0, 1, tzinfo=timezone.utc),
            }]
            dt = date(2025, 3, 5)
            path = events_path(base_path, str(tenant.id), str(source.id), dt)
            path.parent.mkdir(parents=True, exist_ok=True)
            write_events_parquet(path, events)

            from services.worker.jobs.materialize import run_materialize_job
            run_materialize_job(
                job_id="j1",
                tenant_id=tenant.id,
                range_start=dt,
                range_end=dt,
                feature_refs=[{"name": "amount", "version": 1}],
                base_path=base_path,
                database_url=sync_url,
                redis_url="redis://localhost:6379/0",
            )

            session.close()
            engine.dispose()

        mock_write.assert_called()
        call_args = mock_write.call_args[0]
        assert call_args[1] == "amount"
        assert call_args[2] == 1
        assert len(call_args[3]) >= 1  # at least one row


def test_perf_helper_50_features() -> None:
    """Basic perf test helper: pipeline/MGET for <=50 features (not strict)."""
    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    mock_redis.pipeline.return_value = mock_pipe

    rows = [(f"u{i}", i, "2025-03-05T10:37:00") for i in range(10)]
    write_features_sync(mock_redis, "t1", "amount", 1, rows)

    # 10 entities * 2 keys = 20 pipeline ops
    assert mock_pipe.set.call_count == 20
    mock_pipe.execute.assert_called_once()


@pytest.mark.asyncio
async def test_get_features_perf_50_refs() -> None:
    """Basic perf: MGET for <=50 feature_refs (not strict)."""
    mock_redis = AsyncMock()
    payload = b'{"value": "1", "feature_ts": "2025-03-05T10:37:00"}'
    mock_redis.mget = AsyncMock(return_value=[payload] * 50)
    mock_redis.aclose = AsyncMock()

    feature_refs = [{"name": f"f{i}", "version": 1} for i in range(50)]
    results = await get_features_async(
        mock_redis,
        "t1",
        {"user_id": "u1"},
        feature_refs,
        "2025-03-05T10:37:00",
    )

    assert len(results) == 50
    assert mock_redis.mget.call_count == 2  # bucket + current
