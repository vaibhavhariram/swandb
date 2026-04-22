#!/usr/bin/env python3
"""
Redis latency benchmark: 10K get operations, 30 features per entity.

Measures p50/p95/p99 latency in ms for online feature serving via get_features_async.
Self-seeding: writes 100 entities × 30 features to Redis before benchmarking.

Usage:
    REDIS_URL=redis://localhost:6379/0 python scripts/benchmark_redis.py
"""

import asyncio
import json
import os
import statistics
import sys
import time
from pathlib import Path

import redis
import redis.asyncio as aioredis

sys.path.insert(0, str(Path(__file__).parent.parent))

from swandb.online.store import get_features_async, write_features_sync

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
N_OPS = 10_000
N_ENTITIES = 100
CONCURRENCY = 50

FEATURE_NAMES = [
    "transaction_amount", "item_price", "item_quantity", "merchant_category", "payment_method_code",
    "last_tx_amount", "last_login_region", "last_device_type", "last_session_duration", "last_cart_value",
    "tx_count_1h",  "tx_sum_1h",  "tx_avg_1h",  "tx_max_1h",  "tx_last_1h",
    "tx_count_24h", "tx_sum_24h", "tx_avg_24h", "tx_max_24h", "tx_last_24h",
    "tx_count_7d",  "tx_sum_7d",  "tx_avg_7d",  "tx_max_7d",  "tx_last_7d",
    "tx_count_30d", "tx_sum_30d", "tx_avg_30d", "tx_max_30d", "tx_last_30d",
]
FEATURE_REFS = [{"name": n, "version": 1} for n in FEATURE_NAMES]
AS_OF_TS = "2026-04-20T12:00:00"


def _load_tenant_id() -> str:
    state_path = Path(__file__).parent / "seed_state.json"
    if state_path.exists():
        return json.loads(state_path.read_text())["tenant_id"]
    return "benchmark-tenant"


def _seed_redis(tenant_id: str) -> None:
    """Write 100 entities × 30 features to Redis (2 keys per: current + bucket)."""
    sync_client = redis.from_url(REDIS_URL)
    for feat_name in FEATURE_NAMES:
        rows = [(f"user_{i:03d}", str(round(i * 10.5, 2)), AS_OF_TS) for i in range(N_ENTITIES)]
        write_features_sync(sync_client, tenant_id, feat_name, 1, rows, ttl_seconds=7200)
    sync_client.close()
    total_keys = N_ENTITIES * len(FEATURE_NAMES) * 2
    print(f"✓ seeded {N_ENTITIES} entities × {len(FEATURE_NAMES)} features ({total_keys:,} Redis keys)")


def _percentile(sorted_vals: list[float], p: float) -> float:
    idx = max(0, int(len(sorted_vals) * p / 100) - 1)
    return sorted_vals[min(idx, len(sorted_vals) - 1)]


async def _run_benchmark(tenant_id: str) -> list[float]:
    async_client = aioredis.from_url(REDIS_URL, decode_responses=False)

    # warm-up: 200 ops (not measured)
    for i in range(200):
        await get_features_async(
            async_client, tenant_id,
            {"user_id": f"user_{i % N_ENTITIES:03d}"},
            FEATURE_REFS, AS_OF_TS,
        )

    sem = asyncio.Semaphore(CONCURRENCY)

    async def timed_get(i: int) -> float:
        entity_id = f"user_{i % N_ENTITIES:03d}"
        async with sem:
            t0 = time.perf_counter()
            await get_features_async(
                async_client, tenant_id,
                {"user_id": entity_id},
                FEATURE_REFS, AS_OF_TS,
            )
            return (time.perf_counter() - t0) * 1000

    print(f"running {N_OPS:,} operations (concurrency={CONCURRENCY}, {len(FEATURE_REFS)} features/op)...")
    latencies = await asyncio.gather(*[timed_get(i) for i in range(N_OPS)])
    await async_client.aclose()
    return sorted(latencies)


async def main() -> None:
    tenant_id = _load_tenant_id()
    print(f"tenant:   {tenant_id}")
    print(f"redis:    {REDIS_URL}")
    print()

    _seed_redis(tenant_id)
    sorted_latencies = await _run_benchmark(tenant_id)

    p50 = _percentile(sorted_latencies, 50)
    p95 = _percentile(sorted_latencies, 95)
    p99 = _percentile(sorted_latencies, 99)
    avg = statistics.mean(sorted_latencies)
    minimum = sorted_latencies[0]
    maximum = sorted_latencies[-1]

    print(f"\n--- Redis online serving latency ({N_OPS:,} ops, {len(FEATURE_REFS)} features/entity) ---")
    print(f"  p50:  {p50:.2f} ms")
    print(f"  p95:  {p95:.2f} ms")
    print(f"  p99:  {p99:.2f} ms")
    print(f"  mean: {avg:.2f} ms")
    print(f"  min:  {minimum:.2f} ms  max: {maximum:.2f} ms")
    print()

    if p95 < 8.0:
        print(f"✓ p95 {p95:.2f}ms < 8ms  (resume claim confirmed)")
    else:
        print(f"! p95 {p95:.2f}ms >= 8ms  (update resume p95 number to {p95:.0f}ms)")


if __name__ == "__main__":
    asyncio.run(main())
