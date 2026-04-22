#!/usr/bin/env python3
"""
Parity validation: 1000-sample comparison of offline parquet vs online Redis.

Uses the 25 last_value + window_agg features from seed_features.py.
Passthrough features are excluded because their feature parquet uses an entity_keys
JSON column (not entity_id), which the offline lookup in parity.py doesn't handle.

Usage:
    python scripts/run_parity.py

Requires:
    scripts/seed_state.json written by scripts/seed_features.py
"""

import asyncio
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from swandb.validate.parity import run_parity_validation

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

# last_value (5) + window_agg 1h/24h/7d/30d × 5 ops (20) = 25 features
PARITY_FEATURE_REFS = [
    {"name": "last_tx_amount",        "version": 1},
    {"name": "last_login_region",     "version": 1},
    {"name": "last_device_type",      "version": 1},
    {"name": "last_session_duration", "version": 1},
    {"name": "last_cart_value",       "version": 1},
    {"name": "tx_count_1h",           "version": 1},
    {"name": "tx_sum_1h",             "version": 1},
    {"name": "tx_avg_1h",             "version": 1},
    {"name": "tx_max_1h",             "version": 1},
    {"name": "tx_last_1h",            "version": 1},
    {"name": "tx_count_24h",          "version": 1},
    {"name": "tx_sum_24h",            "version": 1},
    {"name": "tx_avg_24h",            "version": 1},
    {"name": "tx_max_24h",            "version": 1},
    {"name": "tx_last_24h",           "version": 1},
    {"name": "tx_count_7d",           "version": 1},
    {"name": "tx_sum_7d",             "version": 1},
    {"name": "tx_avg_7d",             "version": 1},
    {"name": "tx_max_7d",             "version": 1},
    {"name": "tx_last_7d",            "version": 1},
    {"name": "tx_count_30d",          "version": 1},
    {"name": "tx_sum_30d",            "version": 1},
    {"name": "tx_avg_30d",            "version": 1},
    {"name": "tx_max_30d",            "version": 1},
    {"name": "tx_last_30d",           "version": 1},
]


async def main() -> None:
    state_path = Path(__file__).parent / "seed_state.json"
    if not state_path.exists():
        print("ERROR: seed_state.json not found. Run scripts/seed_features.py first.")
        sys.exit(1)

    state = json.loads(state_path.read_text())
    tenant_id = state["tenant_id"]
    base_path = Path(state["base_path"])
    redis_url = state.get("redis_url", REDIS_URL)

    print(f"tenant:     {tenant_id}")
    print(f"base_path:  {base_path}")
    print(f"features:   {len(PARITY_FEATURE_REFS)} (last_value + window_agg)")
    print(f"sample:     1000 windows, threshold: 1%")
    print()

    result = await run_parity_validation(
        base_path=base_path,
        tenant_id=tenant_id,
        feature_refs=PARITY_FEATURE_REFS,
        entity_keys=["user_id"],
        redis_url=redis_url,
        sample_size=1000,
        threshold=0.01,
    )

    print(f"sample_size:    {result['sample_size']}")
    print(f"mismatch_count: {result['mismatch_count']}")
    print(f"mismatch_rate:  {result['mismatch_rate']:.4f}  ({result['mismatch_rate'] * 100:.2f}%)")
    print(f"status:         {result['status']}")
    print()

    if result["status"] == "passed":
        print(f"✓ parity passed: {result['mismatch_rate'] * 100:.2f}% < 1% across {result['sample_size']} samples")
    else:
        print(f"✗ parity FAILED: {result['mismatch_rate'] * 100:.2f}% > 1%")
        if result.get("details"):
            print("\nfirst mismatches:")
            for d in result["details"][:5]:
                print(f"  entity={d['entity_key_values']}  feat={d['feature']}")
                print(f"    offline={d['offline']}  online={d['online']}")

    sys.exit(0 if result["status"] == "passed" else 1)


if __name__ == "__main__":
    asyncio.run(main())
