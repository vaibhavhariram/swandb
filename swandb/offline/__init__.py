"""Offline storage: parquet layout and event writing."""

from swandb.offline.layout import events_path
from swandb.offline.writer import write_events_parquet

__all__ = ["events_path", "write_events_parquet"]
