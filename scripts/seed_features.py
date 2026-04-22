#!/usr/bin/env python3
"""
Seed 30 features through the full SwanDB pipeline (ingest → materialize → Redis).

Usage:
    DATABASE_URL=postgresql://chronosdb:chronosdb@localhost:5432/chronosdb \\
    REDIS_URL=redis://localhost:6379/0 \\
    python scripts/seed_features.py

Writes scripts/seed_state.json for use by benchmark_redis.py and run_parity.py.
"""

import json
import os
import random
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).parent.parent))

from swandb.db.base import Base
from swandb.db.models import Feature, FeatureVersion, Source, Tenant
from swandb.offline.layout import events_path
from swandb.offline.writer import write_events_parquet
from swandb.transforms.spec import compute_spec_hash
from services.worker.jobs.materialize import run_materialize_job

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://chronosdb:chronosdb@localhost:5432/chronosdb"
)
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
BASE_PATH = Path(os.environ.get("BASE_PATH", "/tmp/swandb_seed"))


def _to_sync_url(url: str) -> str:
    if "+asyncpg" in url:
        return url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
    if url.startswith("postgresql://") and "+" not in url.split("//")[1]:
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


# 30 features: 5 passthrough + 5 last_value + 20 window_agg (5 ops × 4 windows)
FEATURE_SPECS: list[tuple[str, dict]] = [
    # passthrough (raw field extraction)
    ("transaction_amount",    {"type": "passthrough", "field": "amount",              "entity_key": "user_id"}),
    ("item_price",            {"type": "passthrough", "field": "price",               "entity_key": "user_id"}),
    ("item_quantity",         {"type": "passthrough", "field": "quantity",            "entity_key": "user_id"}),
    ("merchant_category",     {"type": "passthrough", "field": "merchant_category",   "entity_key": "user_id"}),
    ("payment_method_code",   {"type": "passthrough", "field": "payment_method_code", "entity_key": "user_id"}),
    # last_value (latest field value per entity)
    ("last_tx_amount",        {"type": "last_value", "field": "amount",               "entity_key": "user_id"}),
    ("last_login_region",     {"type": "last_value", "field": "region",               "entity_key": "user_id"}),
    ("last_device_type",      {"type": "last_value", "field": "device_type",          "entity_key": "user_id"}),
    ("last_session_duration", {"type": "last_value", "field": "session_duration",     "entity_key": "user_id"}),
    ("last_cart_value",       {"type": "last_value", "field": "cart_value",           "entity_key": "user_id"}),
    # window_agg 1h
    ("tx_count_1h",   {"type": "window_agg", "op": "count",                             "entity_key": "user_id", "window": "1h"}),
    ("tx_sum_1h",     {"type": "window_agg", "op": "sum",   "field": "amount",          "entity_key": "user_id", "window": "1h"}),
    ("tx_avg_1h",     {"type": "window_agg", "op": "avg",   "field": "amount",          "entity_key": "user_id", "window": "1h"}),
    ("tx_max_1h",     {"type": "window_agg", "op": "max",   "field": "amount",          "entity_key": "user_id", "window": "1h"}),
    ("tx_last_1h",    {"type": "window_agg", "op": "last",  "field": "amount",          "entity_key": "user_id", "window": "1h"}),
    # window_agg 24h
    ("tx_count_24h",  {"type": "window_agg", "op": "count",                             "entity_key": "user_id", "window": "24h"}),
    ("tx_sum_24h",    {"type": "window_agg", "op": "sum",   "field": "amount",          "entity_key": "user_id", "window": "24h"}),
    ("tx_avg_24h",    {"type": "window_agg", "op": "avg",   "field": "amount",          "entity_key": "user_id", "window": "24h"}),
    ("tx_max_24h",    {"type": "window_agg", "op": "max",   "field": "amount",          "entity_key": "user_id", "window": "24h"}),
    ("tx_last_24h",   {"type": "window_agg", "op": "last",  "field": "amount",          "entity_key": "user_id", "window": "24h"}),
    # window_agg 7d
    ("tx_count_7d",   {"type": "window_agg", "op": "count",                             "entity_key": "user_id", "window": "7d"}),
    ("tx_sum_7d",     {"type": "window_agg", "op": "sum",   "field": "amount",          "entity_key": "user_id", "window": "7d"}),
    ("tx_avg_7d",     {"type": "window_agg", "op": "avg",   "field": "amount",          "entity_key": "user_id", "window": "7d"}),
    ("tx_max_7d",     {"type": "window_agg", "op": "max",   "field": "amount",          "entity_key": "user_id", "window": "7d"}),
    ("tx_last_7d",    {"type": "window_agg", "op": "last",  "field": "amount",          "entity_key": "user_id", "window": "7d"}),
    # window_agg 30d
    ("tx_count_30d",  {"type": "window_agg", "op": "count",                             "entity_key": "user_id", "window": "30d"}),
    ("tx_sum_30d",    {"type": "window_agg", "op": "sum",   "field": "amount",          "entity_key": "user_id", "window": "30d"}),
    ("tx_avg_30d",    {"type": "window_agg", "op": "avg",   "field": "amount",          "entity_key": "user_id", "window": "30d"}),
    ("tx_max_30d",    {"type": "window_agg", "op": "max",   "field": "amount",          "entity_key": "user_id", "window": "30d"}),
    ("tx_last_30d",   {"type": "window_agg", "op": "last",  "field": "amount",          "entity_key": "user_id", "window": "30d"}),
]


def _generate_events(n_users: int = 50, n_events: int = 500, days_back: int = 35) -> list[dict]:
    """Generate synthetic transaction events spread over days_back days."""
    rng = random.Random(42)
    regions = ["us-west", "us-east", "eu-west", "ap-south"]
    devices = ["mobile", "desktop", "tablet"]
    merchants = ["grocery", "electronics", "clothing", "dining", "travel"]
    methods = ["credit", "debit", "paypal", "crypto"]

    now = datetime.now(timezone.utc)
    events = []
    for i in range(n_events):
        user_id = f"user_{rng.randint(1, n_users):03d}"
        offset_secs = rng.randint(0, days_back * 86400)
        event_ts = now - timedelta(seconds=offset_secs)
        amount = round(rng.uniform(1.0, 500.0), 2)
        events.append({
            "event_id": str(uuid.uuid4()),
            "event_hash": f"hash_{i}",
            "entity_keys": {"user_id": user_id},
            "event_ts": event_ts,
            "event_type": "transaction",
            "payload": {
                "amount": amount,
                "price": round(amount * rng.uniform(0.8, 1.2), 2),
                "quantity": rng.randint(1, 10),
                "merchant_category": rng.choice(merchants),
                "payment_method_code": rng.choice(methods),
                "region": rng.choice(regions),
                "device_type": rng.choice(devices),
                "session_duration": rng.randint(30, 3600),
                "cart_value": round(amount * rng.uniform(1.0, 3.0), 2),
            },
            "ingest_ts": event_ts + timedelta(seconds=1),
        })
    return events


def main() -> None:
    sync_url = _to_sync_url(DATABASE_URL)
    engine = create_engine(sync_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(engine, expire_on_commit=False)
    session = Session()

    try:
        # Tenant + source
        tenant_name = f"swandb-seed-{uuid.uuid4().hex[:8]}"
        tenant = Tenant(name=tenant_name)
        session.add(tenant)
        session.flush()
        print(f"tenant:  {tenant.id}  ({tenant_name})")

        source = Source(tenant_id=tenant.id, name="transactions", type="file", config={})
        session.add(source)
        session.flush()
        print(f"source:  {source.id}")

        # 30 features + versions
        for feat_name, spec in FEATURE_SPECS:
            feat = Feature(tenant_id=tenant.id, name=feat_name, source_id=source.id)
            session.add(feat)
            session.flush()
            fv = FeatureVersion(
                feature_id=feat.id,
                version=1,
                transform_spec=spec,
                spec_hash=compute_spec_hash(spec),
            )
            session.add(fv)
            session.flush()

        session.commit()
        print(f"✓ registered {len(FEATURE_SPECS)} features")

        # Generate events and write per-date parquet partitions
        events = _generate_events()
        by_date: dict = defaultdict(list)
        for ev in events:
            by_date[ev["event_ts"].date()].append(ev)

        total_written = 0
        for dt, batch in sorted(by_date.items()):
            path = events_path(BASE_PATH, str(tenant.id), str(source.id), dt)
            total_written += write_events_parquet(path, batch)

        print(f"✓ wrote {total_written} events across {len(by_date)} date partitions")

        range_start = min(by_date.keys())
        range_end = max(by_date.keys())
        feature_refs = [{"name": name, "version": 1} for name, _ in FEATURE_SPECS]

        print(f"materializing {len(feature_refs)} features over {range_start} → {range_end}...")
        n_events, n_rows = run_materialize_job(
            job_id=str(uuid.uuid4()),
            tenant_id=str(tenant.id),
            range_start=range_start,
            range_end=range_end,
            feature_refs=feature_refs,
            base_path=BASE_PATH,
            database_url=sync_url,
            redis_url=REDIS_URL,
        )
        print(f"✓ {n_rows} feature rows materialized ({n_events} event reads)")

        state = {
            "tenant_id": str(tenant.id),
            "source_id": str(source.id),
            "feature_refs": feature_refs,
            "base_path": str(BASE_PATH),
            "redis_url": REDIS_URL,
            "range_start": range_start.isoformat(),
            "range_end": range_end.isoformat(),
        }
        state_path = Path(__file__).parent / "seed_state.json"
        state_path.write_text(json.dumps(state, indent=2))
        print(f"✓ seed state → {state_path}")

    finally:
        session.close()
        engine.dispose()


if __name__ == "__main__":
    main()
