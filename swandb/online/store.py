"""Online store: write/read feature values from Redis."""

import json
from datetime import datetime
from typing import Any

import pyarrow as pa

from swandb.online.key_format import (
    bucket_key,
    bucket_ts_from_as_of,
    current_key,
)


def _serialize_value(value: Any, feature_ts: str) -> str:
    """Serialize feature value + timestamp for Redis."""
    return json.dumps({"value": value, "feature_ts": feature_ts})


def extract_rows_for_redis(
    table: pa.Table,
    transform_spec: dict[str, Any],
) -> list[tuple[str, Any, str]]:
    """
    Extract (entity_id, value, feature_ts) from transform result table.
    Handles passthrough (entity_keys), last_value (entity_id, as_of_event_ts), window_agg (entity_id, feature_ts).
    """
    cols = table.column_names
    rows = []

    entity_key = transform_spec.get("entity_key", "entity_id")
    ts_col = "feature_ts" if "feature_ts" in cols else "as_of_event_ts" if "as_of_event_ts" in cols else "event_ts"

    for i in range(table.num_rows):
        if "entity_id" in cols:
            entity_id = str(table.column("entity_id")[i])
        elif "entity_keys" in cols:
            ek = table.column("entity_keys")[i]
            ek_str = ek.as_py() if hasattr(ek, "as_py") else str(ek)
            try:
                parsed = json.loads(ek_str)
                entity_id = str(parsed.get(entity_key, ""))
            except Exception:
                entity_id = ""
        else:
            entity_id = ""

        val_col = table.column("value")[i]
        value = val_col.as_py() if hasattr(val_col, "as_py") else val_col

        ts_val = table.column(ts_col)[i]
        if hasattr(ts_val, "as_py"):
            feature_ts = ts_val.as_py()
        else:
            feature_ts = str(ts_val)
        if hasattr(feature_ts, "isoformat"):
            feature_ts = feature_ts.isoformat()

        if entity_id:
            rows.append((entity_id, value, feature_ts))

    return rows


def _deserialize_value(raw: str | bytes | None) -> dict[str, Any] | None:
    """Deserialize Redis value to {value, feature_ts}."""
    if raw is None:
        return None
    s = raw.decode() if isinstance(raw, bytes) else raw
    return json.loads(s)


def write_features_sync(
    redis_client: Any,
    tenant_id: str,
    feature_name: str,
    version: int,
    rows: list[tuple[str, Any, str]],
    ttl_seconds: int = 86400,
) -> int:
    """
    Write feature rows to Redis. Uses pipeline for performance.
    rows: list of (entity_id, value, feature_ts).
    Writes both current key and bucketed as-of key (1-min bucket).
    Returns count of keys written.
    """
    pipe = redis_client.pipeline()
    count = 0

    for entity_id, value, feature_ts in rows:
        payload = _serialize_value(value, feature_ts)
        bucket = bucket_ts_from_as_of(feature_ts)

        k_current = current_key(tenant_id, feature_name, version, entity_id)
        k_bucket = bucket_key(tenant_id, feature_name, version, bucket, entity_id)

        pipe.set(k_current, payload, ex=ttl_seconds)
        pipe.set(k_bucket, payload, ex=ttl_seconds)
        count += 2

    pipe.execute()
    return count


async def get_features_async(
    redis_client: Any,
    tenant_id: str,
    entity_keys: dict[str, str],
    feature_refs: list[dict[str, Any]],
    as_of_ts: datetime | str,
    entity_key: str = "user_id",
) -> list[dict[str, Any]]:
    """
    Fetch features for entity. Uses MGET for performance.
    Returns list of {feature_ref, value, feature_ts, status}.
    status: "ok" if from bucket, "current_fallback" if from current (out-of-window).
    """
    entity_id = entity_keys.get(entity_key)
    if not entity_id:
        return [{"feature_ref": ref, "value": None, "feature_ts": None, "status": "missing_entity"} for ref in feature_refs]

    bucket = bucket_ts_from_as_of(as_of_ts)
    keys_bucket = []
    keys_current = []
    for ref in feature_refs:
        name = ref.get("name")
        version = ref.get("version", 1)
        if not name:
            continue
        keys_bucket.append(bucket_key(tenant_id, name, version, bucket, entity_id))
        keys_current.append(current_key(tenant_id, name, version, entity_id))

    # MGET bucket keys and current keys (for fallback)
    raw_bucket = await redis_client.mget(keys_bucket)
    raw_current = await redis_client.mget(keys_current)

    results = []
    for i, ref in enumerate(feature_refs):
        name = ref.get("name")
        version = ref.get("version", 1)
        if not name:
            results.append({"feature_ref": ref, "value": None, "feature_ts": None, "status": "invalid_ref"})
            continue

        data = _deserialize_value(raw_bucket[i] if i < len(raw_bucket) else None)
        if data is not None:
            results.append({
                "feature_ref": ref,
                "value": data.get("value"),
                "feature_ts": data.get("feature_ts"),
                "status": "ok",
            })
            continue

        # Fallback to current
        data_current = _deserialize_value(raw_current[i] if i < len(raw_current) else None)
        if data_current is not None:
            results.append({
                "feature_ref": ref,
                "value": data_current.get("value"),
                "feature_ts": data_current.get("feature_ts"),
                "status": "current_fallback",
            })
        else:
            results.append({
                "feature_ref": ref,
                "value": None,
                "feature_ts": None,
                "status": "not_found",
            })

    return results
