"""FastAPI application for ChronosDB API service."""

from contextlib import asynccontextmanager

import asyncpg
import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, FastAPI
from fastapi.responses import JSONResponse

from chronosdb.db.base import init_async_engine
from services.api import ingest, materialize, registry
from services.api.auth import AuthContext, require_api_key
from services.api.config import settings
from services.api.middleware import (
    LoggingMiddleware,
    RequestIdMiddleware,
    configure_structlog,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: setup and teardown."""
    configure_structlog()
    init_async_engine(settings.database_url)
    from services.worker.queue import start_materialize_worker

    start_materialize_worker(settings.database_url, settings.offline_objects_path)
    yield
    # Teardown handled by dependency cleanup


app = FastAPI(
    title="ChronosDB API",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(LoggingMiddleware)
app.add_middleware(RequestIdMiddleware)

# Tenant-scoped v1 API (requires API key)
v1_router = APIRouter(prefix="/v1/{tenant_id}", tags=["v1"])


async def _check_postgres() -> bool:
    """Check Postgres connectivity."""
    try:
        conn = await asyncpg.connect(settings.database_url)
        await conn.close()
        return True
    except Exception:
        return False


async def _check_redis() -> bool:
    """Check Redis connectivity."""
    try:
        client = aioredis.from_url(settings.redis_url)
        await client.ping()
        await client.aclose()
        return True
    except Exception:
        return False


@app.get("/healthz")
async def healthz() -> dict:
    """Liveness probe: returns 200 if the process is running."""
    return {"status": "ok"}


@app.get("/readyz")
async def readyz() -> JSONResponse:
    """Readiness probe: returns 200 if Postgres and Redis are reachable."""
    postgres_ok = await _check_postgres()
    redis_ok = await _check_redis()

    if postgres_ok and redis_ok:
        return JSONResponse(
            content={"status": "ok", "postgres": "ok", "redis": "ok"},
            status_code=200,
        )

    details = {
        "status": "degraded",
        "postgres": "ok" if postgres_ok else "unreachable",
        "redis": "ok" if redis_ok else "unreachable",
    }
    return JSONResponse(content=details, status_code=503)


@v1_router.get("/healthz")
async def v1_healthz(auth: AuthContext = Depends(require_api_key)) -> dict:
    """Tenant-scoped health check. Requires valid API key."""
    return {"status": "ok", "tenant_id": auth.tenant_id}


v1_router.include_router(registry.router)
v1_router.include_router(ingest.router)
v1_router.include_router(materialize.router)
app.include_router(v1_router)
