# Getting Started

## Quick start

```bash
# Start Postgres + Redis
cd infra && docker-compose up -d

# Install and run API
poetry install
poetry run uvicorn services.api.main:app --reload --port 8000

# In another terminal: run tests
poetry run pytest -v
```

## Environment variables

| Variable       | Default                                      | Description        |
|----------------|----------------------------------------------|--------------------|
| `DATABASE_URL` | `postgresql://chronosdb:chronosdb@localhost:5432/chronosdb` | Postgres connection |
| `REDIS_URL`    | `redis://localhost:6379/0`                   | Redis connection   |
| `OFFLINE_OBJECTS_PATH` | `offline`                         | Base path for parquet storage |

## Authentication

Tenant-scoped endpoints (`/v1/{tenant_id}/...`) require:

```
Authorization: Bearer <api_key>
```

The API key is validated against the `api_keys` table (hashed with SHA-256). Path `tenant_id` must match the key's tenant.
