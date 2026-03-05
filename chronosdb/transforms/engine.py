"""Transform engine: dispatch by spec type."""

from typing import Any

import duckdb

from chronosdb.transforms.last_value import last_value
from chronosdb.transforms.passthrough import passthrough
from chronosdb.transforms.spec import compute_spec_hash

_TRANSFORMS: dict[str, Any] = {
    "passthrough": passthrough,
    "last_value": last_value,
}


def apply_transform(
    conn: duckdb.DuckDBPyConnection,
    input_rel: duckdb.DuckDBPyRelation,
    transform_spec: dict[str, Any],
    *,
    feature_version: int = 1,
    as_of: str | None = None,
) -> duckdb.DuckDBPyRelation:
    """
    Apply transform by spec type. Output includes spec_hash and feature_version.
    """
    transform_type = transform_spec.get("type", "")
    if not transform_type or transform_type not in _TRANSFORMS:
        raise ValueError(f"Unknown transform type: {transform_type}")

    spec_hash = compute_spec_hash(transform_spec)
    fn = _TRANSFORMS[transform_type]

    return fn(
        conn,
        input_rel,
        transform_spec,
        spec_hash=spec_hash,
        feature_version=feature_version,
        as_of=as_of,
    )
