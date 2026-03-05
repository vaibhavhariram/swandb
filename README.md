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

## API endpoints

| Endpoint   | Description                                      |
|-----------|--------------------------------------------------|
| `GET /healthz` | Liveness probe (process running)              |
| `GET /readyz`  | Readiness probe (Postgres + Redis reachable) |
