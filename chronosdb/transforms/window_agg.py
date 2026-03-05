"""Window aggregation transform: rolling window ops over event stream."""

import re
import uuid
from typing import Any

import duckdb
import pyarrow as pa


# Supported window formats: 1h, 24h, 7d, 30d
_WINDOW_PATTERN = re.compile(r"^(\d+)(h|d|m)$", re.IGNORECASE)


def _parse_window(window_str: str) -> str:
    """Parse window string (e.g. '1h', '24h', '7d') to DuckDB INTERVAL literal."""
    m = _WINDOW_PATTERN.match(window_str.strip())
    if not m:
        raise ValueError(
            f"Invalid window '{window_str}'. Use format: 1h, 24h, 7d, 30d"
        )
    num, unit = m.groups()
    unit = unit.lower()
    if unit == "h":
        return f"INTERVAL '{num} hour'"
    if unit == "d":
        return f"INTERVAL '{num} day'"
    if unit == "m":
        return f"INTERVAL '{num} minute'"
    raise ValueError(f"Unknown window unit: {unit}")


def window_agg(
    conn: duckdb.DuckDBPyConnection,
    input_rel: duckdb.DuckDBPyRelation,
    spec: dict[str, Any],
    *,
    spec_hash: str,
    feature_version: int,
    as_of: str | None = None,
) -> duckdb.DuckDBPyRelation:
    """
    Rolling window aggregation over events. Window: (label_ts - window, label_ts] inclusive.
    Event exactly at label_ts: included. Event after label_ts: excluded.

    Spec: {
        "type": "window_agg",
        "op": "count" | "sum" | "avg" | "max" | "last",
        "field": "field_name",  # required for sum, avg, max, last
        "entity_key": "user_id",
        "window": "24h"  # 1h, 24h, 7d, 30d
    }
    Output: entity_id, value, feature_ts, spec_hash, feature_version.
    """
    if not as_of:
        raise ValueError("window_agg requires as_of timestamp (label_ts)")

    op = spec.get("op", "").lower()
    if op not in ("count", "sum", "avg", "max", "last"):
        raise ValueError(f"window_agg op must be count|sum|avg|max|last, got: {op}")

    field = spec.get("field", "")
    if op != "count" and not field:
        raise ValueError(f"window_agg op '{op}' requires 'field'")

    entity_key = spec.get("entity_key", "entity_id")
    window_str = spec.get("window", "")
    if not window_str:
        raise ValueError("window_agg requires 'window' (e.g. 1h, 24h, 7d)")

    interval = _parse_window(window_str)

    view_name = f"_windowagg_{uuid.uuid4().hex[:8]}"
    arrow_reader = input_rel.arrow()
    arrow_table = (
        arrow_reader.read_all()
        if hasattr(arrow_reader, "read_all")
        else pa.Table.from_batches(list(arrow_reader))
    )
    conn.register(view_name, arrow_table)

    try:
        # Window: (label_ts - window, label_ts] inclusive
        # event_ts > label_ts - window AND event_ts <= label_ts
        label_ts = f"CAST('{as_of}' AS TIMESTAMP)"
        window_start = f"({label_ts} - {interval})"

        entity_expr = f"json_extract_string(entity_keys, '$.{entity_key}')"

        if op == "count":
            agg_expr = "COUNT(*)::DOUBLE"
        elif op == "last":
            # last: use ROW_NUMBER to pick latest by event_ts, ingest_ts
            agg_expr = None  # handled separately
        else:
            json_path = f"$.{field}"
            val_expr = f"CAST(json_extract_string(payload, '{json_path}') AS DOUBLE)"
            if op == "sum":
                agg_expr = f"SUM({val_expr})"
            elif op == "avg":
                agg_expr = f"AVG({val_expr})"
            elif op == "max":
                agg_expr = f"MAX({val_expr})"
            else:
                agg_expr = "NULL"

        if op == "last":
            query = f"""
            WITH filtered AS (
                SELECT
                    {entity_expr} AS entity_id,
                    json_extract_string(payload, '$.{field}') AS value,
                    event_ts,
                    ingest_ts,
                    ROW_NUMBER() OVER (
                        PARTITION BY {entity_expr}
                        ORDER BY event_ts DESC, ingest_ts DESC
                    ) AS rn
                FROM {view_name}
                WHERE event_ts > {window_start} AND event_ts <= {label_ts}
            )
            SELECT
                entity_id,
                value,
                {label_ts} AS feature_ts,
                '{spec_hash}' AS spec_hash,
                {feature_version}::INTEGER AS feature_version
            FROM filtered
            WHERE rn = 1
            """
        else:
            query = f"""
            SELECT
                {entity_expr} AS entity_id,
                {agg_expr} AS value,
                {label_ts} AS feature_ts,
                '{spec_hash}' AS spec_hash,
                {feature_version}::INTEGER AS feature_version
            FROM {view_name}
            WHERE event_ts > {window_start} AND event_ts <= {label_ts}
            GROUP BY {entity_expr}
            """

        result = conn.sql(query)
        res_arrow = result.arrow()
        res_table = (
            res_arrow.read_all()
            if hasattr(res_arrow, "read_all")
            else pa.Table.from_batches(list(res_arrow))
        )
        return conn.from_arrow(res_table)
    finally:
        conn.unregister(view_name)
