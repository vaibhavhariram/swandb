"""Last-value transform: last-known value <= as_of from event stream."""

import uuid
from typing import Any

import duckdb
import pyarrow as pa


def last_value(
    conn: duckdb.DuckDBPyConnection,
    input_rel: duckdb.DuckDBPyRelation,
    spec: dict[str, Any],
    *,
    spec_hash: str,
    feature_version: int,
    as_of: str | None = None,
) -> duckdb.DuckDBPyRelation:
    """
    Last-known value <= as_of per entity. Tie-break by ingest_ts if event_ts ties.
    Spec: {"type": "last_value", "field": "field_name", "entity_key": "user_id"}
    Output: entity_id, value, as_of_event_ts, as_of_ingest_ts, spec_hash, feature_version.
    """
    if not as_of:
        raise ValueError("last_value requires as_of timestamp")

    field = spec.get("field", "")
    entity_key = spec.get("entity_key", "entity_id")
    if not field:
        raise ValueError("last_value spec requires 'field'")

    view_name = f"_lastvalue_{uuid.uuid4().hex[:8]}"
    arrow_reader = input_rel.arrow()
    arrow_table = arrow_reader.read_all() if hasattr(arrow_reader, "read_all") else pa.Table.from_batches(list(arrow_reader))
    conn.register(view_name, arrow_table)
    try:
        query = f"""
    WITH filtered AS (
        SELECT * FROM {view_name}
        WHERE event_ts <= CAST('{as_of}' AS TIMESTAMP)
    ),
    ranked AS (
        SELECT
            json_extract_string(entity_keys, '$.{entity_key}') AS entity_id,
            json_extract_string(payload, '$.{field}') AS value,
            event_ts,
            ingest_ts,
            ROW_NUMBER() OVER (
                PARTITION BY json_extract_string(entity_keys, '$.{entity_key}')
                ORDER BY event_ts DESC, ingest_ts DESC
            ) AS rn
        FROM filtered
    )
    SELECT
        entity_id,
        value,
        event_ts AS as_of_event_ts,
        ingest_ts AS as_of_ingest_ts,
        '{spec_hash}' AS spec_hash,
        {feature_version}::INTEGER AS feature_version
    FROM ranked
    WHERE rn = 1
    """
        result = conn.sql(query)
        res_arrow = result.arrow()
        res_table = res_arrow.read_all() if hasattr(res_arrow, "read_all") else pa.Table.from_batches(list(res_arrow))
        return conn.from_arrow(res_table)
    finally:
        conn.unregister(view_name)
