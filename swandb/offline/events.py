"""Event deduplication: event_id and event_hash."""

import hashlib
import json
from datetime import datetime
from typing import Any


def _canonical_json(obj: Any) -> str:
    """Serialize to canonical JSON."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def compute_event_hash(
    entity_keys: dict[str, Any] | list[Any],
    event_ts: datetime,
    event_type: str,
    payload: dict[str, Any] | Any,
) -> str:
    """
    Compute event_hash from (entity_keys, event_ts, event_type, payload).
    Used when event_id is not provided.
    """
    # Normalize for hashing
    ts_str = event_ts.isoformat() if hasattr(event_ts, "isoformat") else str(event_ts)
    blob = _canonical_json({
        "entity_keys": entity_keys if isinstance(entity_keys, (dict, list)) else {},
        "event_ts": ts_str,
        "event_type": event_type or "",
        "payload": payload if isinstance(payload, (dict, list)) else (payload or {}),
    })
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def dedupe_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Dedupe events: prefer event_id if provided, else use event_hash.
    Returns deduplicated list (first occurrence wins).
    """
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for e in events:
        key = e.get("event_id")
        if key:
            if key in seen:
                continue
            seen.add(key)
        else:
            key = e.get("event_hash")
            if not key:
                continue
            if key in seen:
                continue
            seen.add(key)
        result.append(e)
    return result
