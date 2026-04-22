"""
In-process job queue for MVP. No Celery.

Jobs are enqueued via materialize_enqueue() and processed by a background
worker thread. The worker updates MaterializationJob status and counters.
"""

import uuid
import queue
import threading
from datetime import date
from typing import Any

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from swandb.db.models import MaterializationJob

from services.worker.jobs.materialize import run_materialize_job


def _to_sync_url(url: str) -> str:
    """Convert asyncpg URL to sync psycopg URL."""
    if "+asyncpg" in url:
        return url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
    if url.startswith("postgresql://") and "+" not in url.split("//")[1]:
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


_job_queue: queue.Queue[dict[str, Any]] = queue.Queue()
_worker_thread: threading.Thread | None = None


def materialize_enqueue(payload: dict[str, Any]) -> None:
    """Enqueue a materialization job. Payload: job_id, tenant_id, range_start, range_end, feature_refs, base_path, database_url."""
    _job_queue.put(payload)


def _worker_loop(
    database_url: str,
    base_path: str,
    redis_url: str | None = None,
) -> None:
    """Process jobs from the queue."""
    engine = create_engine(_to_sync_url(database_url))
    SessionLocal = sessionmaker(engine, expire_on_commit=False)

    while True:
        try:
            payload = _job_queue.get()
            if payload is None:
                break

            job_id = payload["job_id"]
            job_uuid = uuid.UUID(str(job_id)) if isinstance(job_id, str) else job_id
            tenant_id = payload["tenant_id"]
            range_start = payload["range_start"]
            range_end = payload["range_end"]
            feature_refs = payload["feature_refs"]

            if isinstance(range_start, str):
                range_start = date.fromisoformat(range_start)
            if isinstance(range_end, str):
                range_end = date.fromisoformat(range_end)

            session = SessionLocal()
            try:
                job = session.execute(
                    select(MaterializationJob).where(MaterializationJob.id == job_uuid)
                ).scalar_one_or_none()
                if not job:
                    continue

                try:
                    event_count, feature_row_count = run_materialize_job(
                        job_id=str(job_id),
                        tenant_id=tenant_id,
                        range_start=range_start,
                        range_end=range_end,
                        feature_refs=feature_refs,
                        base_path=base_path,
                        database_url=database_url,
                        redis_url=redis_url,
                    )
                    job.status = "succeeded"
                    job.event_count = event_count
                    job.feature_row_count = feature_row_count
                except Exception:
                    job.status = "failed"
                    job.event_count = None
                    job.feature_row_count = None
                finally:
                    session.commit()
            finally:
                session.close()
        except Exception:
            # Log and continue
            pass


def start_materialize_worker(
    database_url: str,
    base_path: str,
    redis_url: str | None = None,
) -> None:
    """Start the background worker thread. Call at app startup."""
    global _worker_thread
    if _worker_thread is not None:
        return
    _worker_thread = threading.Thread(
        target=_worker_loop,
        args=(database_url, base_path, redis_url),
        daemon=True,
    )
    _worker_thread.start()


def stop_materialize_worker() -> None:
    """Stop the worker (sends sentinel)."""
    _job_queue.put(None)
