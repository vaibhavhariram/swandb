# Point-in-Time (PIT) Correctness

ChronosDB ensures features are joined to labels using only information available at the label timestamp. No future leakage.

## Training Build

`POST /v1/{tenant}/training/build` produces a training dataset via **ASOF JOIN**:

- **Labels**: Parquet with `entity_id`, `label_ts`, and optionally `label_value` (target).
- **Features**: Parquet with `entity_id`, `value`, `feature_ts` (or `as_of_event_ts`, `event_ts`).
- **Join**: For each label row, select the **latest** feature row where `feature_ts <= label_ts`.

### DuckDB ASOF JOIN

```sql
SELECT labels.*, f.value AS amount_v1
FROM labels
ASOF LEFT JOIN features f
  ON labels.entity_id = f.entity_id
 AND labels.label_ts >= f.feature_ts
```

- **Equality**: Match on `entity_id`.
- **Inequality**: `label_ts >= feature_ts` selects the most recent feature row before each label.

### Output

- **Training parquet**: `offline/training/tenant={tenant}/build={build_id}/part-0000.parquet`
- **Manifest JSON**: `offline/training/tenant={tenant}/build={build_id}/manifest.json`

Manifest includes:

- `feature_refs`: Features joined
- `spec_hashes`: Deterministic hashes for lineage
- `row_count`: Number of training rows

### Labels Schema

| Column | Required | Description |
|--------|----------|-------------|
| entity_id | Yes* | Entity identifier (*or entity_keys with entity_key) |
| label_ts | Yes | Timestamp of the label (point-in-time) |
| label_value | No | Target value for supervised learning |

If `entity_keys` (JSON) is used instead of `entity_id`, specify `entity_key` (e.g. `user_id`) in the request.

### Feature Schema

Feature parquet must have:

- `entity_id` or `entity_keys`
- `value`
- `feature_ts` or `as_of_event_ts` or `event_ts`

## Golden Test

`tests/test_training_pit.py::test_pit_join_golden` verifies:

- Labels at t1, t2; features at t0, t1, t2.
- Join picks last feature where `feature_ts <= label_ts`.
- No future leakage.

## Transform Semantics

| Transform | feature_ts | Semantics |
|-----------|------------|-----------|
| passthrough | event_ts | Value from event at that time |
| last_value | as_of_event_ts | Last known value ≤ as_of |
| window_agg | feature_ts | Aggregation over (label_ts - window, label_ts] |
