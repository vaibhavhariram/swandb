# Feature Transforms

Transforms convert raw events into feature rows. Each transform has a spec (JSON) and produces deterministic output stamped with `spec_hash` and `feature_version`.

## Transform Types

| Type | Description | Requires `as_of` |
|------|-------------|------------------|
| passthrough | Extract field from payload | No |
| last_value | Last-known value ≤ as_of per entity | Yes |
| window_agg | Rolling window aggregation | Yes |

---

## passthrough

Extracts a field from the event payload and emits one feature row per event.

### Spec

```json
{
  "type": "passthrough",
  "field": "amount"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| field | Yes | JSON path key in payload (e.g. `amount`, `score`) |

### Output

| Column | Description |
|--------|-------------|
| entity_keys | JSON string of entity identifiers |
| value | Extracted value (string) |
| event_ts | Event timestamp |
| ingest_ts | Ingestion timestamp |
| spec_hash | Deterministic hash of spec |
| feature_version | Version number |

### Example

```json
// Event: {"entity_keys": {"user_id": "u1"}, "payload": {"amount": 100}}
// Output: entity_keys='{"user_id":"u1"}', value='100', event_ts=..., ingest_ts=...
```

---

## last_value

Returns the last-known value for each entity at a point in time (`as_of`). Events with `event_ts > as_of` are excluded. Tie-break by `ingest_ts` when `event_ts` ties.

### Spec

```json
{
  "type": "last_value",
  "field": "amount",
  "entity_key": "user_id"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| field | Yes | JSON path key in payload |
| entity_key | No | Entity key in entity_keys (default: `entity_id`) |

### Output

| Column | Description |
|--------|-------------|
| entity_id | Extracted entity identifier |
| value | Last value (string) |
| as_of_event_ts | Timestamp of the selected event |
| as_of_ingest_ts | Ingest timestamp of the selected event |
| spec_hash | Deterministic hash of spec |
| feature_version | Version number |

### Semantics

- **Included**: Events with `event_ts <= as_of`
- **Selection**: Per entity, pick event with max `(event_ts, ingest_ts)`

---

## window_agg

Rolling window aggregation over events. Computes one value per entity for a given label timestamp (`as_of`).

### Spec

```json
{
  "type": "window_agg",
  "op": "sum",
  "field": "amount",
  "entity_key": "user_id",
  "window": "24h"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| op | Yes | `count`, `sum`, `avg`, `max`, or `last` |
| field | Yes* | JSON path key in payload (*not required for `count`) |
| entity_key | No | Entity key in entity_keys (default: `entity_id`) |
| window | Yes | Window size: `1h`, `24h`, `7d`, `30d`, `60m` |

### Operations

| Op | Description | Field required |
|----|-------------|----------------|
| count | Count of events in window | No |
| sum | Sum of numeric field | Yes |
| avg | Average of numeric field | Yes |
| max | Maximum of numeric field | Yes |
| last | Most recent value (by event_ts, ingest_ts) | Yes |

### Window Semantics

- **Window**: `(label_ts - window, label_ts]` — left-exclusive, right-inclusive
- **label_ts** = `as_of` (the point in time we compute the feature for)
- **feature_ts** = label_ts in output (event time used as feature timestamp)

### Boundary Conditions

| Condition | Included? |
|-----------|-----------|
| Event exactly at `label_ts` | **Yes** |
| Event after `label_ts` | **No** |
| Event at `label_ts - window` | **No** (left boundary exclusive) |
| Event just after `label_ts - window` | **Yes** |

### Output

| Column | Description |
|--------|-------------|
| entity_id | Extracted entity identifier |
| value | Aggregated value (numeric for count/sum/avg/max; string for last) |
| feature_ts | Label timestamp (`as_of`) — when the feature is valid |
| spec_hash | Deterministic hash of spec |
| feature_version | Version number |

### Examples

**Count events in last hour**

```json
{
  "type": "window_agg",
  "op": "count",
  "entity_key": "user_id",
  "window": "1h"
}
```

**Sum of amounts in last 24 hours**

```json
{
  "type": "window_agg",
  "op": "sum",
  "field": "amount",
  "entity_key": "user_id",
  "window": "24h"
}
```

**Last score in window (most recent event)**

```json
{
  "type": "window_agg",
  "op": "last",
  "field": "score",
  "entity_key": "user_id",
  "window": "7d"
}
```

### Window Format

- `1h`, `24h` — hours
- `7d`, `30d` — days
- `60m` — minutes

---

## Determinism

All transforms produce deterministic output for a given spec and input:

- **spec_hash**: SHA-256 of canonical JSON of the transform spec. Used for cache invalidation and lineage.
- **feature_version**: Integer version from the registry. Incremented when a new version is created.
