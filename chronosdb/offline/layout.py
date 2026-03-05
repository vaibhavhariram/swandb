"""Offline storage path layout."""

from pathlib import Path
from datetime import date


def events_path(
    base_dir: str | Path,
    tenant_id: str,
    source_id: str,
    dt: date,
    part: str = "part-0000",
) -> Path:
    """
    Return path for events parquet file.
    Layout: offline/events/tenant={tenant}/source={source_id}/dt=YYYY-MM-DD/part-0000.parquet
    """
    base = Path(base_dir)
    dt_str = dt.strftime("%Y-%m-%d")
    return base / "events" / f"tenant={tenant_id}" / f"source={source_id}" / f"dt={dt_str}" / f"{part}.parquet"
