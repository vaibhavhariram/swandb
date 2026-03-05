# Offline/Online Parity

ChronosDB ensures the same feature values are returned by offline (parquet) and online (Redis) stores for a given entity and point-in-time.

## Validation

`POST /v1/{tenant}/validate/parity`:

1. **Sample** N (entity_id, as_of_ts) pairs from offline feature parquet.
2. **Fetch offline**: Read parquet, apply PIT logic (last value where feature_ts ≤ as_of_ts).
3. **Fetch online**: Use Redis bucketed key for as_of_ts (1-minute bucket).
4. **Compare**: Offline vs online.
5. **Record**: Write to `quality_results` table.
6. **Fail**: If `mismatch_rate > threshold`.

### Request

```json
{
  "feature_refs": [{"name": "amount", "version": 1}],
  "sample_size": 100,
  "threshold": 0.0
}
```

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
| details | Sample of mismatches (first 10) |

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
