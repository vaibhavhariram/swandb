# Online Feature Store (Redis)

Low-latency feature serving via Redis. Features are written during materialization and read via `POST /v1/{tenant}/features/get`.

## Key Format

Centralized in `chronosdb/online/key_format.py`:

| Key Type | Format | Example |
|----------|--------|---------|
| Current | `chronosdb:{tenant}:{feature}:v{version}:current:{entity_id}` | `chronosdb:t1:amount:v1:current:u1` |
| Bucketed | `chronosdb:{tenant}:{feature}:v{version}:b:{bucket_ts}:{entity_id}` | `chronosdb:t1:amount:v1:b:2025-03-05T10:37:00:u1` |

- **Current**: Latest feature value (always overwritten on materialization).
- **Bucketed**: Point-in-time value for 1-minute bucket. `bucket_ts` = `as_of_ts` floored to minute (e.g. `2025-03-05T10:37:22` → `2025-03-05T10:37:00`).

## Value Format

JSON: `{"value": <any>, "feature_ts": "<iso timestamp>"}`

- `value`: Feature value (string, number, etc.).
- `feature_ts`: Timestamp when the feature is valid (event time / label time).

## Write Path

During materialization (`services/worker/jobs/materialize.py`):

1. After writing feature parquet, rows are extracted as `(entity_id, value, feature_ts)`.
2. For each row, both keys are written:
   - **Current key**: Always updated.
   - **Bucketed key**: Written with `bucket_ts = floor(feature_ts)`.
3. Uses Redis **Pipeline** for batch writes.
4. Requires `redis_url` in config; skipped if not set.

## Read Path

`POST /v1/{tenant}/features/get`:

- **Request**: `entity_keys`, `feature_refs`, `as_of_ts`
- **Lookup order**:
  1. Bucket key for `as_of_ts` (floored to minute).
  2. If not found, fall back to **current** key.
- **Response**: `features[]` with `value`, `feature_ts`, `status`.
- Uses **MGET** for batch reads (≤50 features recommended).

### Status

| Status | Meaning |
|--------|---------|
| `ok` | Value from bucketed key (exact as_of) |
| `current_fallback` | Bucket key missing; used current (out-of-window) |
| `not_found` | No key found |
| `missing_entity` | `entity_keys` missing required key |
| `invalid_ref` | Invalid feature_ref |

## TTL

- Default: **86400 seconds** (24 hours).
- Configurable via `online_ttl_seconds` in materialize job.
- Keys expire automatically; no manual cleanup.

## Bucket Semantics

- **Granularity**: 1 minute.
- **Lookup**: `as_of_ts` is floored to the start of the minute.
- **Fallback**: If the bucket key does not exist (e.g. no materialization for that minute), the **current** key is used and `status: "current_fallback"` is returned.
- **Limitation**: Fallback to current may return a value from a different point in time; clients should check `status` and `feature_ts`.

## Limitations

1. **Single entity per request**: `POST /features/get` fetches for one entity (one `entity_keys`).
2. **Entity key**: Default `user_id`; other entity keys not yet configurable per request.
3. **No batch entities**: Multiple entities require multiple requests.
4. **Feature count**: Designed for ≤50 features per request; no strict enforcement.
5. **PIT accuracy**: Bucketed keys provide PIT only for materialized minutes; gaps fall back to current.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| REDIS_URL | `redis://localhost:6379/0` | Redis connection URL |
