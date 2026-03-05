"""Unit tests for transform engine using in-memory DuckDB."""

from datetime import datetime, timezone

import duckdb
import pytest

from chronosdb.transforms.engine import apply_transform
from chronosdb.transforms.spec import compute_spec_hash


def _events_table(conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    """Create in-memory events table for tests."""
    conn.execute("""
        CREATE TABLE events (
            event_id VARCHAR,
            event_hash VARCHAR,
            entity_keys VARCHAR,
            event_ts TIMESTAMP,
            event_type VARCHAR,
            payload VARCHAR,
            ingest_ts TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO events VALUES
        ('e1', 'h1', '{"user_id": "u1"}', '2025-03-05 10:00:00', 'click', '{"amount": 100}', '2025-03-05 10:00:01'),
        ('e2', 'h2', '{"user_id": "u1"}', '2025-03-05 11:00:00', 'click', '{"amount": 200}', '2025-03-05 11:00:01'),
        ('e3', 'h3', '{"user_id": "u2"}', '2025-03-05 10:30:00', 'click', '{"amount": 50}', '2025-03-05 10:30:01')
    """)
    return conn.table("events")


def test_passthrough_extracts_correctly() -> None:
    """Passthrough extracts field from payload correctly."""
    conn = duckdb.connect(":memory:")
    input_rel = _events_table(conn)

    spec = {"type": "passthrough", "field": "amount"}
    result = apply_transform(conn, input_rel, spec)

    rows = result.fetchall()
    assert len(rows) == 3

    # Check extracted values
    amounts = [r[1] for r in rows]  # value column
    assert "100" in amounts
    assert "200" in amounts
    assert "50" in amounts

    # Check metadata columns
    spec_hash = compute_spec_hash(spec)
    for row in rows:
        assert row[4] == spec_hash  # spec_hash column
        assert row[5] == 1  # feature_version column


def test_last_value_chooses_correct_record_by_event_ts() -> None:
    """Last-value chooses correct record by event_ts <= as_of."""
    conn = duckdb.connect(":memory:")
    input_rel = _events_table(conn)

    spec = {"type": "last_value", "field": "amount", "entity_key": "user_id"}
    as_of = "2025-03-05 10:45:00"  # Before e2 (11:00), after e1 (10:00) and e3 (10:30)
    result = apply_transform(conn, input_rel, spec, as_of=as_of)

    rows = result.fetchall()
    # u1: e1 (10:00) is last <= 10:45 -> amount 100
    # u2: e3 (10:30) is last <= 10:45 -> amount 50
    assert len(rows) == 2

    by_entity = {r[0]: r[1] for r in rows}
    assert by_entity["u1"] == "100"
    assert by_entity["u2"] == "50"


def test_last_value_tie_break_by_ingest_ts() -> None:
    """Last-value tie-breaks by ingest_ts when event_ts ties."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE events (
            event_id VARCHAR,
            event_hash VARCHAR,
            entity_keys VARCHAR,
            event_ts TIMESTAMP,
            event_type VARCHAR,
            payload VARCHAR,
            ingest_ts TIMESTAMP
        )
    """)
    # Same event_ts, different ingest_ts - later ingest_ts wins
    conn.execute("""
        INSERT INTO events VALUES
        ('e1', 'h1', '{"user_id": "u1"}', '2025-03-05 10:00:00', 'x', '{"score": 1}', '2025-03-05 10:00:01'),
        ('e2', 'h2', '{"user_id": "u1"}', '2025-03-05 10:00:00', 'x', '{"score": 2}', '2025-03-05 10:00:02')
    """)
    input_rel = conn.table("events")

    spec = {"type": "last_value", "field": "score", "entity_key": "user_id"}
    result = apply_transform(conn, input_rel, spec, as_of="2025-03-05 10:00:05")

    rows = result.fetchall()
    assert len(rows) == 1
    assert rows[0][1] == "2"  # e2 has later ingest_ts, score 2 wins


def test_passthrough_output_has_spec_hash_and_feature_version() -> None:
    """Passthrough output is stamped with spec_hash and feature_version."""
    conn = duckdb.connect(":memory:")
    input_rel = _events_table(conn)

    spec = {"type": "passthrough", "field": "amount"}
    result = apply_transform(conn, input_rel, spec, feature_version=3)

    cols = [d[0] for d in result.description]
    assert "spec_hash" in cols
    assert "feature_version" in cols

    rows = result.fetchall()
    expected_hash = compute_spec_hash(spec)
    for row in rows:
        idx_spec = cols.index("spec_hash")
        idx_ver = cols.index("feature_version")
        assert row[idx_spec] == expected_hash
        assert row[idx_ver] == 3


# --- window_agg ---


def test_window_agg_event_exactly_at_label_ts_included() -> None:
    """Event exactly at label_ts is included in the window."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE events (
            event_id VARCHAR, event_hash VARCHAR, entity_keys VARCHAR,
            event_ts TIMESTAMP, event_type VARCHAR, payload VARCHAR, ingest_ts TIMESTAMP
        )
    """)
    # Single event at 10:00:00; label_ts = 10:00:00
    conn.execute("""
        INSERT INTO events VALUES
        ('e1', 'h1', '{"user_id": "u1"}', '2025-03-05 10:00:00', 'x', '{"amount": 100}', '2025-03-05 10:00:01')
    """)
    input_rel = conn.table("events")

    spec = {"type": "window_agg", "op": "count", "entity_key": "user_id", "window": "1h"}
    result = apply_transform(conn, input_rel, spec, as_of="2025-03-05 10:00:00")

    rows = result.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "u1"  # entity_id
    assert rows[0][1] == 1.0   # count


def test_window_agg_event_after_label_ts_excluded() -> None:
    """Event after label_ts is excluded from the window."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE events (
            event_id VARCHAR, event_hash VARCHAR, entity_keys VARCHAR,
            event_ts TIMESTAMP, event_type VARCHAR, payload VARCHAR, ingest_ts TIMESTAMP
        )
    """)
    # Event at 10:05:00; label_ts = 10:00:00, window 1h -> (09:00, 10:00]
    conn.execute("""
        INSERT INTO events VALUES
        ('e1', 'h1', '{"user_id": "u1"}', '2025-03-05 10:05:00', 'x', '{"amount": 100}', '2025-03-05 10:05:01')
    """)
    input_rel = conn.table("events")

    spec = {"type": "window_agg", "op": "count", "entity_key": "user_id", "window": "1h"}
    result = apply_transform(conn, input_rel, spec, as_of="2025-03-05 10:00:00")

    rows = result.fetchall()
    assert len(rows) == 0


def test_window_agg_sum() -> None:
    """Window agg sum aggregates correctly."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE events (
            event_id VARCHAR, event_hash VARCHAR, entity_keys VARCHAR,
            event_ts TIMESTAMP, event_type VARCHAR, payload VARCHAR, ingest_ts TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO events VALUES
        ('e1', 'h1', '{"user_id": "u1"}', '2025-03-05 09:30:00', 'x', '{"amount": 10}', '2025-03-05 09:30:01'),
        ('e2', 'h2', '{"user_id": "u1"}', '2025-03-05 10:00:00', 'x', '{"amount": 20}', '2025-03-05 10:00:01'),
        ('e3', 'h3', '{"user_id": "u2"}', '2025-03-05 09:45:00', 'x', '{"amount": 5}', '2025-03-05 09:45:01')
    """)
    input_rel = conn.table("events")

    spec = {"type": "window_agg", "op": "sum", "field": "amount", "entity_key": "user_id", "window": "1h"}
    result = apply_transform(conn, input_rel, spec, as_of="2025-03-05 10:00:00")

    rows = result.fetchall()
    assert len(rows) == 2
    by_entity = {r[0]: r[1] for r in rows}
    assert by_entity["u1"] == 30.0  # 10 + 20
    assert by_entity["u2"] == 5.0


def test_window_agg_last() -> None:
    """Window agg last returns most recent value in window."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE events (
            event_id VARCHAR, event_hash VARCHAR, entity_keys VARCHAR,
            event_ts TIMESTAMP, event_type VARCHAR, payload VARCHAR, ingest_ts TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO events VALUES
        ('e1', 'h1', '{"user_id": "u1"}', '2025-03-05 09:30:00', 'x', '{"score": 1}', '2025-03-05 09:30:01'),
        ('e2', 'h2', '{"user_id": "u1"}', '2025-03-05 10:00:00', 'x', '{"score": 2}', '2025-03-05 10:00:01')
    """)
    input_rel = conn.table("events")

    spec = {"type": "window_agg", "op": "last", "field": "score", "entity_key": "user_id", "window": "1h"}
    result = apply_transform(conn, input_rel, spec, as_of="2025-03-05 10:00:00")

    rows = result.fetchall()
    assert len(rows) == 1
    assert rows[0][1] == "2"  # last by event_ts


def test_window_agg_avg_and_max() -> None:
    """Window agg avg and max work correctly."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE events (
            event_id VARCHAR, event_hash VARCHAR, entity_keys VARCHAR,
            event_ts TIMESTAMP, event_type VARCHAR, payload VARCHAR, ingest_ts TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO events VALUES
        ('e1', 'h1', '{"user_id": "u1"}', '2025-03-05 09:30:00', 'x', '{"amount": 10}', '2025-03-05 09:30:01'),
        ('e2', 'h2', '{"user_id": "u1"}', '2025-03-05 10:00:00', 'x', '{"amount": 20}', '2025-03-05 10:00:01')
    """)
    input_rel = conn.table("events")

    spec = {"type": "window_agg", "op": "avg", "field": "amount", "entity_key": "user_id", "window": "1h"}
    result = apply_transform(conn, input_rel, spec, as_of="2025-03-05 10:00:00")
    rows = result.fetchall()
    assert len(rows) == 1
    assert rows[0][1] == 15.0  # (10+20)/2

    spec_max = {"type": "window_agg", "op": "max", "field": "amount", "entity_key": "user_id", "window": "1h"}
    result_max = apply_transform(conn, input_rel, spec_max, as_of="2025-03-05 10:00:00")
    rows_max = result_max.fetchall()
    assert rows_max[0][1] == 20.0
