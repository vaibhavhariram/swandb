"""Integration test: ingest events -> register feature+version -> materialize -> feature parquet exists."""

import os
import tempfile
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from chronosdb.offline.layout import events_path, features_path
from chronosdb.offline.writer import write_events_parquet
from services.worker.jobs.materialize import run_materialize_job


def _get_test_db_url() -> str:
    """Use DATABASE_URL if Postgres; else skip. Prefer psycopg over psycopg2."""
    url = os.environ.get(
        "DATABASE_URL",
        "postgresql://chronosdb:chronosdb@localhost:5432/chronosdb",
    )
    if "postgresql" not in url:
        pytest.skip("Integration test requires Postgres (DATABASE_URL)")
    # Use psycopg (v3) if plain postgresql:// - project has psycopg, not psycopg2
    if url.startswith("postgresql://") and "+" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


@pytest.fixture
def temp_offline_path():
    with tempfile.TemporaryDirectory() as tmp:
        yield Path(tmp)


def test_materialize_integration(temp_offline_path):
    """
    Full flow: create tenant/source/feature/version, write events parquet,
    run materialize job, assert feature parquet exists and is correct.
    Requires Postgres (DATABASE_URL) and psycopg. Skips if DB unreachable.
    """
    db_url = _get_test_db_url()
    base_path = temp_offline_path

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from chronosdb.db.base import Base
    from chronosdb.db.models import Tenant, Source, Feature, FeatureVersion

    # Sync URL for worker (psycopg)
    sync_url = db_url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)

    try:
        engine = create_engine(sync_url)
        engine.connect().close()
    except Exception as e:
        pytest.skip(f"Cannot connect to Postgres: {e}")

    Base.metadata.create_all(engine)
    Session = sessionmaker(engine, expire_on_commit=False)
    session = Session()

    try:
        tenant = Tenant(name=f"test-mat-{uuid.uuid4().hex[:8]}")
        session.add(tenant)
        session.flush()

        source = Source(
            tenant_id=tenant.id,
            name="test-source",
            type="file",
            config={},
        )
        session.add(source)
        session.flush()

        feature = Feature(
            tenant_id=tenant.id,
            name="amount_feature",
            source_id=source.id,
        )
        session.add(feature)
        session.flush()

        fv = FeatureVersion(
            feature_id=feature.id,
            version=1,
            transform_spec={"type": "passthrough", "field": "amount"},
            spec_hash="abc123",
        )
        session.add(fv)
        session.commit()

        # Write events parquet
        events = [
            {
                "event_id": "e1",
                "event_hash": "h1",
                "entity_keys": {"user_id": "u1"},
                "event_ts": datetime(2025, 3, 5, 10, 0, 0, tzinfo=timezone.utc),
                "event_type": "click",
                "payload": {"amount": 100},
                "ingest_ts": datetime(2025, 3, 5, 10, 0, 1, tzinfo=timezone.utc),
            },
            {
                "event_id": "e2",
                "event_hash": "h2",
                "entity_keys": {"user_id": "u2"},
                "event_ts": datetime(2025, 3, 5, 10, 30, 0, tzinfo=timezone.utc),
                "event_type": "click",
                "payload": {"amount": 50},
                "ingest_ts": datetime(2025, 3, 5, 10, 30, 1, tzinfo=timezone.utc),
            },
        ]
        dt = date(2025, 3, 5)
        path = events_path(base_path, str(tenant.id), str(source.id), dt)
        path.parent.mkdir(parents=True, exist_ok=True)
        write_events_parquet(path, events)
        assert path.exists()

        # Run materialize job
        event_count, feature_row_count = run_materialize_job(
            job_id=str(uuid.uuid4()),
            tenant_id=tenant.id,
            range_start=dt,
            range_end=dt,
            feature_refs=[{"name": "amount_feature", "version": 1}],
            base_path=base_path,
            database_url=sync_url,
        )

        assert event_count == 2
        assert feature_row_count == 2

        fp = features_path(base_path, str(tenant.id), "amount_feature", 1, dt)
        assert fp.exists(), f"Feature parquet not found at {fp}"

        table = pq.read_table(fp)
        assert "value" in table.column_names
        assert "spec_hash" in table.column_names
        assert "feature_version" in table.column_names
        assert table.num_rows == 2
        values = table.column("value")
        assert "100" in [str(v) for v in values]
        assert "50" in [str(v) for v in values]
    finally:
        session.close()
        engine.dispose()
