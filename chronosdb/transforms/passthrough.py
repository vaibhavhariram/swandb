"""Passthrough transform: extract field from payload and emit feature rows."""

import uuid
from typing import Any

import duckdb
import pyarrow as pa


def passthrough(
    conn: duckdb.DuckDBPyConnection,
    input_rel: duckdb.DuckDBPyRelation,
    spec: dict[str, Any],
    *,
    spec_hash: str,
    feature_version: int,
    as_of: str | None = None,
) -> duckdb.DuckDBPyRelation:
    """
    Extract field from payload and emit feature rows.
    Spec: {"type": "passthrough", "field": "field_name"}
    Output: entity_keys, value, event_ts, ingest_ts, spec_hash, feature_version.
    """
    field = spec.get("field", "")
    if not field:
        raise ValueError("passthrough spec requires 'field'")

    view_name = f"_passthrough_{uuid.uuid4().hex[:8]}"
    arrow_reader = input_rel.arrow()
    arrow_table = arrow_reader.read_all() if hasattr(arrow_reader, "read_all") else pa.Table.from_batches(list(arrow_reader))
    conn.register(view_name, arrow_table)
    try:
        json_path = f"$.{field}"
        query = f"""
        SELECT
            entity_keys,
            json_extract_string(payload, '{json_path}') AS value,
            event_ts,
            ingest_ts,
            '{spec_hash}' AS spec_hash,
            {feature_version}::INTEGER AS feature_version
        FROM {view_name}
        """
        result = conn.sql(query)
        # Materialize so result is independent of registered view
        res_arrow = result.arrow()
        res_table = res_arrow.read_all() if hasattr(res_arrow, "read_all") else pa.Table.from_batches(list(res_arrow))
        return conn.from_arrow(res_table)
    finally:
        conn.unregister(view_name)
