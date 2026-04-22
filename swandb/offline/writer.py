"""Write events to parquet using pyarrow."""

from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


# Schema: event_id (optional), event_hash, entity_keys (JSONB), event_ts, event_type, payload (JSONB), ingest_ts
EVENTS_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("event_hash", pa.string()),
    ("entity_keys", pa.string()),  # JSON string
    ("event_ts", pa.timestamp("us", tz="UTC")),
    ("event_type", pa.string()),
    ("payload", pa.string()),  # JSON string
    ("ingest_ts", pa.timestamp("us", tz="UTC")),
])


def write_events_parquet(
    path: Path,
    events: list[dict[str, Any]],
    schema: pa.Schema | None = None,
) -> int:
    """
    Write events to parquet file. Creates parent dirs. Returns count written.
    """
    if not events:
        return 0

    schema = schema or EVENTS_SCHEMA
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Build columns
    event_ids = [e.get("event_id") or "" for e in events]
    event_hashes = [e["event_hash"] for e in events]
    import json

    def _to_json(v):
        return json.dumps(v) if isinstance(v, (dict, list)) else (str(v) if v else "{}")

    entity_keys = [_to_json(e.get("entity_keys")) for e in events]
    event_ts = [e["event_ts"] for e in events]
    event_type = [e.get("event_type") or "" for e in events]
    payload = [_to_json(e.get("payload")) for e in events]
    ingest_ts = [e["ingest_ts"] for e in events]

    table = pa.table({
        "event_id": event_ids,
        "event_hash": event_hashes,
        "entity_keys": entity_keys,
        "event_ts": event_ts,
        "event_type": event_type,
        "payload": payload,
        "ingest_ts": ingest_ts,
    }, schema=schema)

    pq.write_table(table, path)
    return len(events)
