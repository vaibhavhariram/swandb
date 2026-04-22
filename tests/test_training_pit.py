"""Golden PIT correctness test for training build."""

import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from swandb.training.build import build_training_dataset


def test_pit_join_golden() -> None:
    """
    Golden test: ASOF JOIN produces correct PIT values.
    Labels at t1, t2; features at t0, t1, t2. Join must pick last feature <= label_ts.
    """
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        tid = "test-tenant"

        # Feature parquet: entity_id, value, feature_ts
        feat_dir = base / "features" / f"tenant={tid}" / "feature=amount" / "version=1"
        feat_dir.mkdir(parents=True)
        feat_table = pa.table({
            "entity_id": ["u1", "u1", "u1", "u2"],
            "value": ["10", "20", "30", "5"],
            "feature_ts": [
                datetime(2025, 3, 5, 10, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 3, 5, 10, 30, 0, tzinfo=timezone.utc),
                datetime(2025, 3, 5, 11, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 3, 5, 10, 15, 0, tzinfo=timezone.utc),
            ],
        })
        (feat_dir / "dt=2025-03-05").mkdir(parents=True)
        pq.write_table(feat_table, feat_dir / "dt=2025-03-05" / "part-0000.parquet")

        # Labels: entity_id, label_ts
        # u1 at 10:45 -> should get value 20 (last feature_ts <= 10:45)
        # u1 at 11:30 -> should get value 30
        # u2 at 10:20 -> should get value 5
        labels_table = pa.table({
            "entity_id": ["u1", "u1", "u2"],
            "label_ts": [
                datetime(2025, 3, 5, 10, 45, 0, tzinfo=timezone.utc),
                datetime(2025, 3, 5, 11, 30, 0, tzinfo=timezone.utc),
                datetime(2025, 3, 5, 10, 20, 0, tzinfo=timezone.utc),
            ],
            "label_value": [1, 0, 1],
        })
        labels_path = base / "labels.parquet"
        labels_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(labels_table, labels_path)

        out_path, manifest_path, manifest = build_training_dataset(
            labels_path=labels_path,
            base_path=base,
            tenant_id=tid,
            feature_refs=[{"name": "amount", "version": 1}],
            feature_specs=[{"spec_hash": "abc"}],
        )

        assert out_path.exists()
        assert manifest_path.exists()
        assert manifest["row_count"] == 3

        result = pq.read_table(out_path)
        # Find column amount_v1 (feature value joined)
        amount_col = None
        for c in result.column_names:
            if "amount" in c and "v1" in c:
                amount_col = c
                break
        assert amount_col is not None

        # u1 @ 10:45 -> 20, u1 @ 11:30 -> 30, u2 @ 10:20 -> 5
        values = [result.column(amount_col)[i].as_py() for i in range(3)]
        assert "20" in values or 20 in values
        assert "30" in values or 30 in values
        assert "5" in values or 5 in values
