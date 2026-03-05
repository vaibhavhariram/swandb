# Offline/Online Parity

ChronosDB ensures the same feature values are returned by offline (parquet) and online (Redis) stores for a given entity and point-in-time.

## Validation

`POST /v1/{tenant}/validate/parity`:

1. **Sample** N (entity_key_values, as_of_ts) pairs from offline feature parquet. Entity key-values are a dict (e.g. `{"user_id": "u1"}`).
2. **Fetch offline**: Read parquet, apply PIT logic (last value where feature_ts ≤ as_of_ts).
3. **Fetch online**: Use Redis bucketed key for as_of_ts (1-minute bucket).
4. **Compare**: Offline vs online.
5. **Record**: Write to `quality_results` table.
6. **Fail**: If `mismatch_rate > threshold`.

### Request

```json
{
  "feature_refs": [{"name": "amount", "version": 1}],
  "entity_keys": ["user_id"],
  "sample_size": 100,
  "threshold": 0.0
}
```

- **entity_keys** (optional): List of entity key names, e.g. `["user_id"]` or `["org_id", "user_id"]`. If omitted, derived from the registry (`features.entity_keys`); all feature_refs must use the same entity_keys list. If provided, must match registry exactly; else 400.

### Response

- `status`: `passed` or `failed`
- `mismatch_count`, `mismatch_rate`, `threshold`
- `job_id`: QualityResult ID

### Quality Results Table

| Column | Description |
|--------|-------------|
| job_type | `parity` |
| status | `passed` / `failed` |
| sample_size | Number of samples |
| mismatch_count | Count of mismatches |
| mismatch_rate | mismatch_count / sample_size |
| threshold | Max allowed mismatch_rate |
| details | Sample of mismatches (first 10), each with `entity_key_values`, `as_of_ts`, `feature`, `offline`, `online` |

## Entity Keys

Parity supports arbitrary entity keys (not just `user_id`):

- **Request**: Pass `entity_keys: ["user_id"]` or `["account_id"]` or `["org_id", "user_id"]`.
- **Omitted**: When `entity_keys` is omitted, the API uses `features.entity_keys` from the registry for the first feature. All feature_refs must share the same entity_keys list.
- **Provided**: When `entity_keys` is provided, it must match the registry exactly; otherwise 400.
- **Parquet**: When parquet has `entity_id`, values are mapped as `{entity_keys[0]: entity_id}`. When parquet has `entity_keys` (JSON), values are extracted for the requested keys.
- **Mismatch details**: Each mismatch includes `entity_key_values` (the full dict) for debugging.

## When Parity Holds

1. **Materialization**: Features are written to both offline parquet and Redis during materialization.
2. **Bucket alignment**: Online uses 1-minute buckets; offline has exact timestamps. For as_of_ts within a materialized minute, both should match.
3. **No drift**: If offline is recomputed without re-materializing online, parity can break.

## When Parity Fails

- **Bucket gap**: as_of_ts in a minute with no materialized data; online falls back to current.
- **Stale online**: Redis TTL expired; key missing.
- **Schema mismatch**: Different entity_key or feature version.

## Test

`tests/test_validate_parity.py::test_parity_mismatch_rate_zero`:

- Offline and online return same values.
- `mismatch_rate == 0`, `status == passed`.
