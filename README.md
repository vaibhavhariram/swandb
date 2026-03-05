# ChronosDB

PIT-correct time-series feature store with offline/online parity.

## Repo layout

```
chronosdb/          # Core library
services/api/       # FastAPI service
services/worker/   # Background worker
infra/             # Docker Compose
docs/              # Documentation
tests/             # Pytest tests
```

## Prerequisites

- Python 3.11+
- Docker & Docker Compose

## Local development

### 1. Start dependencies (Postgres, Redis)

```bash
cd infra && docker-compose up -d
```

### 2. Install dependencies

**With Poetry:**
```bash
poetry install
```

**With pip:**
```bash
python -m venv .venv
source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows
pip install -e ".[dev]"
```

### 3. Configure (optional)

Defaults work with `infra/docker-compose.yml`. Override via env vars:

```bash
export DATABASE_URL="postgresql://chronosdb:chronosdb@localhost:5432/chronosdb"
export REDIS_URL="redis://localhost:6379/0"
```

### 4. Run the API

```bash
uvicorn services.api.main:app --reload --host 0.0.0.0 --port 8000
```

### 5. Run tests

```bash
pytest -v
```

### Materialization (MVP)

Materialization jobs use an **in-process queue**: no Celery or external job broker. Jobs are enqueued via `POST /v1/{tenant_id}/materialize` and processed by a background thread in the same process. For production, replace with Celery or similar.

## API endpoints

| Endpoint   | Auth | Description                                      |
|-----------|------|--------------------------------------------------|
| `GET /healthz` | No | Liveness probe (process running)              |
| `GET /readyz`  | No | Readiness probe (Postgres + Redis reachable) |
| `GET /v1/{tenant_id}/healthz` | Bearer token | Tenant-scoped health check |
| `POST /v1/{tenant_id}/registry/features` | Bearer token | Create feature |
| `POST /v1/{tenant_id}/registry/features/{name}/versions` | Bearer token | Create feature version |
| `GET /v1/{tenant_id}/registry/features/{name}` | Bearer token | Get feature with versions |
| `POST /v1/{tenant_id}/ingest/events` | Bearer token | Batch event ingestion |
| `POST /v1/{tenant_id}/materialize` | Bearer token | Enqueue materialization job |

### Authentication

Tenant-scoped endpoints require `Authorization: Bearer <api_key>`. The API key must belong to the tenant in the path.
