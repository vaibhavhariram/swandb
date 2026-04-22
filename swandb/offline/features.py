"""Write feature parquet from transform output."""

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from swandb.offline.layout import features_path


def write_feature_parquet(
    base_dir: str | Path,
    tenant_id: str,
    feature_name: str,
    version: int,
    dt: date,
    table: pa.Table,
    part: str = "part-0000",
) -> int:
    """
    Write feature table to parquet. Creates parent dirs. Returns row count.
    """
    path = features_path(base_dir, tenant_id, feature_name, version, dt, part)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return table.num_rows
