"""Tests for parity validation."""

import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from swandb.validate.parity import run_parity_validation


@pytest.mark.asyncio
async def test_parity_mismatch_rate_zero() -> None:
    """Parity validation passes when offline and online return same values (mismatch_rate=0)."""
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        tid = "test-tenant"

        # Create feature parquet
        feat_dir = base / "features" / f"tenant={tid}" / "feature=amount" / "version=1"
        (feat_dir / "dt=2025-03-05").mkdir(parents=True)
        feat_table = pa.table({
            "entity_id": ["u1", "u2"],
            "value": ["100", "50"],
            "feature_ts": [
                datetime(2025, 3, 5, 10, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 3, 5, 10, 30, 0, tzinfo=timezone.utc),
            ],
        })
        pq.write_table(feat_table, feat_dir / "dt=2025-03-05" / "part-0000.parquet")

        # Mock Redis to return same values as offline
        import json
        payload_u1 = json.dumps({"value": "100", "feature_ts": "2025-03-05T10:00:00+00:00"})
        payload_u2 = json.dumps({"value": "50", "feature_ts": "2025-03-05T10:30:00+00:00"})

        mock_redis = AsyncMock()
        mock_redis.aclose = AsyncMock()

        def mock_mget(keys):
            results = []
            for k in keys:
                if "u1" in str(k):
                    results.append(payload_u1.encode())
                elif "u2" in str(k):
                    results.append(payload_u2.encode())
                else:
                    results.append(None)
            return results

        async def amock_mget(keys):
            return mock_mget(keys)

        mock_redis.mget = amock_mget

        with patch("swandb.validate.parity.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = mock_redis

            result = await run_parity_validation(
                base_path=base,
                tenant_id=tid,
                feature_refs=[{"name": "amount", "version": 1}],
                entity_keys=["user_id"],
                redis_url="redis://localhost:6379/0",
                sample_size=10,
                threshold=0.0,
            )

        assert result["status"] == "passed"
        assert result["mismatch_rate"] == 0.0
        assert result["mismatch_count"] == 0


@pytest.mark.asyncio
async def test_parity_arbitrary_entity_keys() -> None:
    """Parity supports arbitrary entity_keys (e.g. account_id), not just user_id."""
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        tid = "test-tenant"

        feat_dir = base / "features" / f"tenant={tid}" / "feature=balance" / "version=1"
        (feat_dir / "dt=2025-03-05").mkdir(parents=True)
        feat_table = pa.table({
            "entity_id": ["acc1", "acc2"],
            "value": ["1000", "500"],
            "feature_ts": [
                datetime(2025, 3, 5, 10, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 3, 5, 10, 30, 0, tzinfo=timezone.utc),
            ],
        })
        pq.write_table(feat_table, feat_dir / "dt=2025-03-05" / "part-0000.parquet")

        import json
        payload_acc1 = json.dumps({"value": "1000", "feature_ts": "2025-03-05T10:00:00+00:00"})
        payload_acc2 = json.dumps({"value": "500", "feature_ts": "2025-03-05T10:30:00+00:00"})

        mock_redis = AsyncMock()
        mock_redis.aclose = AsyncMock()

        def mock_mget(keys):
            results = []
            for k in keys:
                if "acc1" in str(k):
                    results.append(payload_acc1.encode())
                elif "acc2" in str(k):
                    results.append(payload_acc2.encode())
                else:
                    results.append(None)
            return results

        mock_redis.mget = AsyncMock(side_effect=mock_mget)

        with patch("swandb.validate.parity.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = mock_redis

            result = await run_parity_validation(
                base_path=base,
                tenant_id=tid,
                feature_refs=[{"name": "balance", "version": 1}],
                entity_keys=["account_id"],
                redis_url="redis://localhost:6379/0",
                sample_size=10,
                threshold=0.0,
            )

        assert result["status"] == "passed"
        assert result["mismatch_rate"] == 0.0
        assert result["mismatch_count"] == 0
