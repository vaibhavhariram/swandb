"""Centralized Redis key format for online feature store."""

from datetime import datetime


def current_key(tenant_id: str, feature_name: str, version: int, entity_id: str) -> str:
    """
    Redis key for current (latest) feature value.
    Format: swandb:{tenant}:{feature}:v{version}:current:{entity_id}
    """
    return f"swandb:{tenant_id}:{feature_name}:v{version}:current:{entity_id}"


def bucket_key(
    tenant_id: str,
    feature_name: str,
    version: int,
    bucket_ts: str,
    entity_id: str,
) -> str:
    """
    Redis key for bucketed as-of feature value (1-minute buckets).
    Format: swandb:{tenant}:{feature}:v{version}:b:{bucket_ts}:{entity_id}
    bucket_ts: ISO format truncated to minute, e.g. 2025-03-05T10:37:00
    """
    return f"swandb:{tenant_id}:{feature_name}:v{version}:b:{bucket_ts}:{entity_id}"


def bucket_ts_from_as_of(as_of_ts: datetime | str) -> str:
    """
    Floor as_of_ts to 1-minute bucket.
    Returns ISO format string truncated to minute: 2025-03-05T10:37:00
    """
    if isinstance(as_of_ts, str):
        dt = datetime.fromisoformat(as_of_ts.replace("Z", "+00:00"))
    else:
        dt = as_of_ts
    # Floor to minute
    floored = dt.replace(second=0, microsecond=0)
    return floored.strftime("%Y-%m-%dT%H:%M:%S")
