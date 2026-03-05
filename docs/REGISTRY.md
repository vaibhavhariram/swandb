# Feature Registry

The feature registry stores feature definitions and versioned transform specifications.

## Versioning

- Each feature has a unique `(tenant_id, name)`.
- Versions are monotonically increasing integers (1, 2, 3, ...).
- **Immutability**: Once a version is created, its `transform_spec` cannot be overwritten.
- New versions are always appended; existing versions are never modified.

## spec_hash

Each feature version stores:

- `transform_spec` (JSONB): The transform specification (arbitrary JSON).
- `spec_hash` (Text): SHA-256 hash of the canonical JSON of `transform_spec`.

### Canonical JSON

To ensure stable hashing regardless of key order or whitespace:

```
canonical_json = json.dumps(transform_spec, sort_keys=True, separators=(",", ":"))
spec_hash = sha256(canonical_json.encode("utf-8")).hexdigest()
```

Equivalent JSON objects produce the same `spec_hash`:

- `{"a": 1, "b": 2}` and `{"b": 2, "a": 1}` → same hash
- `{"x": "foo"}` and `{"x": "foo"}` → same hash

Different content produces different hashes:

- `{"a": 1}` vs `{"a": 2}` → different hashes

### Use Cases

- **Deduplication**: Detect identical transform specs across versions or features.
- **Caching**: Use `spec_hash` as a cache key for compiled transforms.
- **Audit**: Verify a version's spec hasn't been tampered with.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/{tenant_id}/registry/features` | Create a feature |
| POST | `/v1/{tenant_id}/registry/features/{name}/versions` | Create a new version |
| GET | `/v1/{tenant_id}/registry/features/{name}` | Get feature with all versions |

All require `Authorization: Bearer <api_key>`.
