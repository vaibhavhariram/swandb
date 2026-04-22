"""Offline/online parity validation."""

import json
import random
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import redis.asyncio as aioredis

from swandb.online.store import get_features_async


def _get_offline_value(
    base_path: Path,
    tenant_id: str,
    feature_name: str,
    version: int,
    entity_id: str,
    as_of_ts: datetime | str,
) -> Any | None:
    """
    Get feature value from offline parquet at as_of_ts.
    Returns last value where feature_ts <= as_of_ts.
    """
    feat_dir = base_path / "features" / f"tenant={tenant_id}" / f"feature={feature_name}" / f"version={version}"
    if not feat_dir.exists():
        return None
    parquet_files = list(feat_dir.rglob("*.parquet"))
    if not parquet_files:
        return None

    tables = [pq.read_table(p) for p in parquet_files]
    import pyarrow as pa
    combined = pa.concat_tables(tables)

    ts_col = "feature_ts" if "feature_ts" in combined.column_names else "as_of_event_ts" if "as_of_event_ts" in combined.column_names else "event_ts"
    entity_col = "entity_id" if "entity_id" in combined.column_names else None
    if not entity_col:
        return None

    if isinstance(as_of_ts, str):
        as_of_dt = datetime.fromisoformat(as_of_ts.replace("Z", "+00:00"))
    else:
        as_of_dt = as_of_ts

    best_val = None
    best_ts = None
    for i in range(combined.num_rows):
        eid = combined.column(entity_col)[i]
        eid_val = eid.as_py() if hasattr(eid, "as_py") else str(eid)
        if str(eid_val) != str(entity_id):
            continue
        ts_val = combined.column(ts_col)[i]
        ts_py = ts_val.as_py() if hasattr(ts_val, "as_py") else ts_val
        if ts_py is None:
            continue
        if hasattr(ts_py, "tzinfo") and ts_py.tzinfo is None and as_of_dt.tzinfo:
            import datetime as dt
            ts_py = ts_py.replace(tzinfo=dt.timezone.utc)
        if ts_py > as_of_dt:
            continue
        if best_ts is None or ts_py > best_ts:
            best_ts = ts_py
            val = combined.column("value")[i]
            best_val = val.as_py() if hasattr(val, "as_py") else val

    return best_val


def _extract_entity_key_values(
    tbl: Any,
    row_idx: int,
    entity_keys: list[str],
) -> dict[str, str] | None:
    """Extract entity_key_values from parquet row. Supports entity_id or entity_keys column."""
    if "entity_id" in tbl.column_names:
        eid = tbl.column("entity_id")[row_idx]
        eid_val = eid.as_py() if hasattr(eid, "as_py") else str(eid)
        if not eid_val or not entity_keys:
            return None
        return {entity_keys[0]: str(eid_val)}
    if "entity_keys" in tbl.column_names:
        ek = tbl.column("entity_keys")[row_idx]
        s = ek.as_py() if hasattr(ek, "as_py") else str(ek)
        parsed = json.loads(s) if isinstance(s, str) else s
        if not isinstance(parsed, dict):
            return None
        return {k: str(parsed[k]) for k in entity_keys if k in parsed}
    return None


async def run_parity_validation(
    base_path: str | Path,
    tenant_id: str,
    feature_refs: list[dict[str, Any]],
    entity_keys: list[str],
    redis_url: str,
    sample_size: int = 100,
    threshold: float = 0.0,
) -> dict[str, Any]:
    """
    Sample entities and as_of_ts, compare offline vs online.
    Returns dict with mismatch_count, mismatch_rate, status, details.
    entity_keys: list of key names, e.g. ["user_id"].
    """
    base_path = Path(base_path)
    tid_str = str(tenant_id)

    # Collect (entity_key_values, as_of_ts, ref) from offline feature parquet
    entity_ts_pairs: list[tuple[dict[str, str], Any, dict]] = []
    for ref in feature_refs:
        name = ref.get("name")
        version = ref.get("version", 1)
        if not name:
            continue
        feat_dir = base_path / "features" / f"tenant={tid_str}" / f"feature={name}" / f"version={version}"
        if not feat_dir.exists():
            continue
        for p in feat_dir.rglob("*.parquet"):
            tbl = pq.read_table(p)
            if "entity_id" not in tbl.column_names and "entity_keys" not in tbl.column_names:
                continue
            if tbl.num_rows == 0:
                continue
            ts_col = "feature_ts" if "feature_ts" in tbl.column_names else "as_of_event_ts" if "as_of_event_ts" in tbl.column_names else "event_ts"
            for i in range(tbl.num_rows):
                ekv = _extract_entity_key_values(tbl, i, entity_keys)
                ts_val = tbl.column(ts_col)[i]
                ts_py = ts_val.as_py() if hasattr(ts_val, "as_py") else ts_val
                if ekv and ts_py:
                    entity_ts_pairs.append((ekv, ts_py, ref))

    if len(entity_ts_pairs) == 0:
        return {
            "sample_size": 0,
            "mismatch_count": 0,
            "mismatch_rate": 0.0,
            "status": "skipped",
            "details": {"reason": "no_offline_data"},
        }

    sample = random.sample(entity_ts_pairs, min(sample_size, len(entity_ts_pairs)))
    client = aioredis.from_url(redis_url)
    mismatches = 0
    details_list = []
    primary_key = entity_keys[0] if entity_keys else "user_id"

    try:
        for entity_key_values, as_of_ts, ref in sample:
            entity_id = entity_key_values.get(primary_key)
            if not entity_id:
                continue
            offline_val = _get_offline_value(
                base_path, tid_str,
                ref["name"], ref.get("version", 1),
                entity_id, as_of_ts,
            )
            online_results = await get_features_async(
                client,
                tid_str,
                entity_key_values,
                [ref],
                as_of_ts,
                entity_key=primary_key,
            )
            online_val = online_results[0]["value"] if online_results else None

            if _values_differ(offline_val, online_val):
                mismatches += 1
                details_list.append({
                    "entity_key_values": entity_key_values,
                    "as_of_ts": as_of_ts.isoformat() if hasattr(as_of_ts, "isoformat") else str(as_of_ts),
                    "feature": ref.get("name"),
                    "offline": offline_val,
                    "online": online_val,
                })

        mismatch_rate = mismatches / len(sample) if sample else 0.0
        status = "failed" if mismatch_rate > threshold else "passed"
        return {
            "sample_size": len(sample),
            "mismatch_count": mismatches,
            "mismatch_rate": mismatch_rate,
            "threshold": threshold,
            "status": status,
            "details": details_list[:10],
        }
    finally:
        await client.aclose()


def _values_differ(a: Any, b: Any) -> bool:
    """Compare values; treat None and missing as equal."""
    if a is None and b is None:
        return False
    if a is None or b is None:
        return True
    return str(a) != str(b)
