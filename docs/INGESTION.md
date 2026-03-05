# Event Ingestion

Batch event ingestion with idempotency and deduplication.

## Endpoint

```
POST /v1/{tenant_id}/ingest/events
Authorization: Bearer <api_key>
```

## Request Schema

```json
{
  "source_id": "uuid",
  "idempotency_key": "string",
  "events": [
    {
      "event_id": "optional-string",
      "entity_keys": { "key": "value" },
      "event_ts": "2025-03-05T12:00:00Z",
      "event_type": "string",
      "payload": { ... }
    }
  ]
}
```

| Field | Required | Description |
|-------|----------|-------------|
| source_id | Yes | Source UUID |
| idempotency_key | Yes | Idempotency key for the batch |
| events | Yes | Non-empty array of events |
| event_id | No | If provided, used for deduplication |
| entity_keys | No | Entity identifiers (dict) |
| event_ts | Yes | Event timestamp (ISO 8601) |
| event_type | No | Event type string |
| payload | No | Arbitrary JSON payload |

## Response

```json
{
  "job_id": "uuid",
  "event_count": 42,
  "max_event_ts": "2025-03-05T12:00:00Z",
  "max_ingest_ts": "2025-03-05T12:01:00Z"
}
```

## Deduplication Rules

1. **Prefer event_id**: If `event_id` is provided, it is used as the deduplication key.
2. **Else event_hash**: If `event_id` is not provided, `event_hash` is computed from:
   - `entity_keys`
   - `event_ts`
   - `event_type`
   - `payload`

   The hash is `sha256(canonical_json({...}))`. Equivalent JSON produces the same hash.

3. **First occurrence wins**: Within a batch, duplicate keys are dropped; the first event is kept.

## Idempotency

- **Key**: `(tenant_id, source_id, idempotency_key)`
- If a prior request with the same key **succeeded**, the response returns the prior `job_id` and counts.
- No new parquet files are written; no duplicate rows.
- Retries are safe: clients can resend the same request without side effects.

## Offline Layout

Events are written to parquet:

```
offline/events/tenant={tenant_id}/source={source_id}/dt=YYYY-MM-DD/part-0000.parquet
```

- Partitioned by `tenant`, `source`, and `dt` (event date).
- Events are grouped by date; one file per partition per batch.
- Schema: `event_id`, `event_hash`, `entity_keys`, `event_ts`, `event_type`, `payload`, `ingest_ts`

## Parquet Schema

| Column | Type | Description |
|--------|------|-------------|
| event_id | string | Optional; empty if not provided |
| event_hash | string | Deduplication hash |
| entity_keys | string | JSON |
| event_ts | timestamp(us, UTC) | Event time |
| event_type | string | Event type |
| payload | string | JSON |
| ingest_ts | timestamp(us, UTC) | Ingestion time |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| OFFLINE_OBJECTS_PATH | `offline` | Base path for parquet storage |
