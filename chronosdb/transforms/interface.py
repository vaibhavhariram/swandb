"""Transform interface: input relation -> output relation."""

from typing import Any, Protocol

import duckdb


class Transform(Protocol):
    """Transform: input DuckDB relation -> output DuckDB relation."""

    def __call__(
        self,
        conn: duckdb.DuckDBPyConnection,
        input_rel: duckdb.DuckDBPyRelation,
        spec: dict[str, Any],
        *,
        spec_hash: str,
        feature_version: int,
        as_of: str | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Apply transform. Returns relation with spec_hash and feature_version columns."""
        ...
