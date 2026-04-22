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


def features_path(
    base_dir: str | Path,
    tenant_id: str,
    feature_name: str,
    version: int,
    dt: date,
    part: str = "part-0000",
) -> Path:
    """
    Return path for feature parquet file.
    Layout: offline/features/tenant={tenant}/feature={name}/version={v}/dt=YYYY-MM-DD/part-0000.parquet
    """
    base = Path(base_dir)
    dt_str = dt.strftime("%Y-%m-%d")
    return base / "features" / f"tenant={tenant_id}" / f"feature={feature_name}" / f"version={version}" / f"dt={dt_str}" / f"{part}.parquet"


def training_path(
    base_dir: str | Path,
    tenant_id: str,
    build_id: str,
    part: str = "part-0000",
) -> Path:
    """
    Return path for training dataset parquet.
    Layout: offline/training/tenant={tenant}/build={build_id}/part-0000.parquet
    """
    base = Path(base_dir)
    return base / "training" / f"tenant={tenant_id}" / f"build={build_id}" / f"{part}.parquet"


def training_manifest_path(
    base_dir: str | Path,
    tenant_id: str,
    build_id: str,
) -> Path:
    """Path for training manifest JSON."""
    base = Path(base_dir)
    return base / "training" / f"tenant={tenant_id}" / f"build={build_id}" / "manifest.json"
