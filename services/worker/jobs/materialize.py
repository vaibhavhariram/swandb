"""
Materialization job: read raw events parquet, run transforms, write feature parquet.

MVP: In-process job runner. No Celery. Jobs are enqueued and run by a background
worker thread in the same process as the API.
"""

import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from chronosdb.db.models import Feature, FeatureVersion
from chronosdb.offline.layout import events_path, features_path
from chronosdb.offline.features import write_feature_parquet
from chronosdb.transforms.engine import apply_transform


def _to_sync_url(url: str) -> str:
    """Convert asyncpg URL to sync psycopg URL."""
    if "+asyncpg" in url:
        return url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
    if url.startswith("postgresql://") and "+" not in url.split("//")[1]:
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


def run_materialize_job(
    job_id: str,
    tenant_id: str,
    range_start: date,
    range_end: date,
    feature_refs: list[dict[str, Any]],
    base_path: str | Path,
    database_url: str,
) -> tuple[int, int]:
    """
    Run materialization: read events, apply transforms, write feature parquet.
    Returns (event_count, feature_row_count).
    """
    base_path = Path(base_path)
    engine = create_engine(_to_sync_url(database_url))
    SessionLocal = sessionmaker(engine, expire_on_commit=False)
    session = SessionLocal()

    try:
        total_events = 0
        total_feature_rows = 0

        for ref in feature_refs:
            name = ref.get("name")
            version = ref.get("version")
            if not name or version is None:
                continue

            # Resolve feature + version
            tid = uuid.UUID(str(tenant_id)) if isinstance(tenant_id, str) else tenant_id
            feature = session.execute(
                select(Feature).where(
                    Feature.tenant_id == tid,
                    Feature.name == name,
                )
            ).scalar_one_or_none()
            if not feature:
                raise ValueError(f"Feature not found: {name}")

            fv = session.execute(
                select(FeatureVersion).where(
                    FeatureVersion.feature_id == feature.id,
                    FeatureVersion.version == version,
                )
            ).scalar_one_or_none()
            if not fv:
                raise ValueError(f"Feature version not found: {name} v{version}")

            source_id = feature.source_id
            if not source_id:
                raise ValueError(f"Feature {name} has no source_id")

            tid_str = str(tid)
            sid_str = str(source_id) if hasattr(source_id, "hex") else source_id

            # Iterate over each dt in range
            dt = range_start
            while dt <= range_end:
                transform_spec = fv.transform_spec
                transform_type = transform_spec.get("type", "")

                if transform_type == "last_value":
                    # last_value needs all events <= end of dt
                    events_table, n_events = _read_events_in_range(
                        base_path, tid_str, sid_str, range_start, dt
                    )
                else:
                    events_table, n_events = _read_events_for_date(
                        base_path, tid_str, sid_str, dt
                    )

                total_events += n_events

                if n_events == 0 and transform_type != "last_value":
                    dt += timedelta(days=1)
                    continue

                conn = duckdb.connect(":memory:")
                input_rel = conn.from_arrow(events_table)

                if transform_type == "last_value":
                    as_of = datetime.combine(
                        dt, datetime.max.time(), tzinfo=timezone.utc
                    ).isoformat()
                    result_rel = apply_transform(
                        conn,
                        input_rel,
                        transform_spec,
                        feature_version=fv.version,
                        as_of=as_of,
                    )
                else:
                    result_rel = apply_transform(
                        conn,
                        input_rel,
                        transform_spec,
                        feature_version=fv.version,
                    )

                res_arrow = result_rel.arrow()
                res_table = (
                    res_arrow.read_all()
                    if hasattr(res_arrow, "read_all")
                    else pa.Table.from_batches(list(res_arrow))
                )
                n_rows = res_table.num_rows
                total_feature_rows += n_rows

                if n_rows > 0:
                    write_feature_parquet(
                        base_path,
                        tid_str,
                        name,
                        fv.version,
                        dt,
                        res_table,
                    )

                dt += timedelta(days=1)

        return (total_events, total_feature_rows)

    finally:
        session.close()
        engine.dispose()


def _read_events_for_date(
    base_path: Path,
    tenant_id: str,
    source_id: str,
    dt: date,
) -> tuple[pa.Table, int]:
    """Read events parquet for a single date. Returns (table, count)."""
    path = events_path(base_path, tenant_id, source_id, dt)
    if not path.exists():
        return (pa.table({}), 0)
    table = pq.read_table(path)
    return (table, table.num_rows)


def _read_events_in_range(
    base_path: Path,
    tenant_id: str,
    source_id: str,
    range_start: date,
    range_end: date,
) -> tuple[pa.Table, int]:
    """Read and concatenate events parquet for date range [range_start, range_end]."""
    tables = []
    dt = range_start
    while dt <= range_end:
        path = events_path(base_path, tenant_id, source_id, dt)
        if path.exists():
            tables.append(pq.read_table(path))
        dt += timedelta(days=1)
    if not tables:
        return (pa.table({}), 0)
    combined = pa.concat_tables(tables)
    return (combined, combined.num_rows)
