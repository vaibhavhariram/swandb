"""PIT-correct training build: ASOF JOIN labels with features."""

import json
import uuid
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from chronosdb.offline.layout import features_path, training_manifest_path, training_path


def _feature_ts_column(cols: list[str]) -> str:
    """Detect feature timestamp column."""
    for c in ("feature_ts", "as_of_event_ts", "event_ts"):
        if c in cols:
            return c
    raise ValueError("Feature table must have feature_ts, as_of_event_ts, or event_ts")


def _ensure_entity_id(table: pa.Table, entity_key: str) -> pa.Table:
    """Ensure table has entity_id column (extract from entity_keys if needed)."""
    if "entity_id" in table.column_names:
        return table
    if "entity_keys" not in table.column_names:
        raise ValueError("Labels must have entity_id or entity_keys")
    import json as _json
    entity_ids = []
    for i in range(table.num_rows):
        ek = table.column("entity_keys")[i]
        s = ek.as_py() if hasattr(ek, "as_py") else str(ek)
        parsed = _json.loads(s) if isinstance(s, str) else s
        entity_ids.append(str(parsed.get(entity_key, "")))
    return table.append_column("entity_id", pa.array(entity_ids))


def build_training_dataset(
    labels_path: str | Path,
    base_path: str | Path,
    tenant_id: str,
    feature_refs: list[dict[str, Any]],
    feature_specs: list[dict[str, Any]],
    entity_key: str = "user_id",
) -> tuple[Path, Path, dict[str, Any]]:
    """
    Build training dataset via ASOF JOIN.
    Returns (output_parquet_path, manifest_path, manifest_dict).
    """
    base_path = Path(base_path)
    labels_path = Path(labels_path)
    if not labels_path.exists():
        raise FileNotFoundError(f"Labels not found: {labels_path}")

    build_id = uuid.uuid4().hex[:12]
    conn = duckdb.connect(":memory:")

    labels_table = pq.read_table(labels_path)
    labels_table = _ensure_entity_id(labels_table, entity_key)
    if "label_ts" not in labels_table.column_names:
        raise ValueError("Labels must have label_ts column")
    conn.register("_labels", labels_table)

    tid_str = str(tenant_id)
    feature_refs_resolved = []
    for i, ref in enumerate(feature_refs):
        name = ref.get("name")
        version = ref.get("version", 1)
        if not name:
            continue
        feat_dir = base_path / "features" / f"tenant={tid_str}" / f"feature={name}" / f"version={version}"
        if not feat_dir.exists():
            raise FileNotFoundError(f"Feature parquet not found: {feat_dir}")
        parquet_files = list(feat_dir.rglob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files in {feat_dir}")
        feat_paths = [str(p) for p in parquet_files]
        feat_rel = conn.read_parquet(feat_paths)
        feat_arrow = feat_rel.arrow()
        feat_table = feat_arrow.read_all() if hasattr(feat_arrow, "read_all") else pa.Table.from_batches(list(feat_arrow))
        feat_cols = feat_table.column_names
        ts_col = _feature_ts_column(feat_cols)
        entity_col = "entity_id" if "entity_id" in feat_cols else "entity_keys"
        if entity_col == "entity_keys":
            feat_table = _ensure_entity_id(feat_table, entity_key)
            entity_col = "entity_id"
        alias = f"_f{i}"
        conn.register(alias, feat_table)
        spec = feature_specs[i] if i < len(feature_specs) else {}
        feature_refs_resolved.append({
            "name": name,
            "version": version,
            "alias": alias,
            "ts_col": ts_col,
            "entity_col": entity_col,
            "value_col": "value",
            "spec_hash": spec.get("spec_hash", ""),
        })

    select_parts = ["_labels.*"]
    join_parts = []
    for fr in feature_refs_resolved:
        alias = fr["alias"]
        ts_col = fr["ts_col"]
        entity_col = fr["entity_col"]
        val_col = fr["value_col"]
        name = fr["name"]
        version = fr["version"]
        select_parts.append(f'{alias}.{val_col} AS "{name}_v{version}"')
        join_parts.append(
            f"ASOF LEFT JOIN {alias} ON _labels.entity_id = {alias}.{entity_col} AND _labels.label_ts >= {alias}.{ts_col}"
        )

    query = f"""
    SELECT {", ".join(select_parts)}
    FROM _labels
    {" ".join(join_parts)}
    """
    result = conn.sql(query)
    res_arrow = result.arrow()
    out_table = res_arrow.read_all() if hasattr(res_arrow, "read_all") else pa.Table.from_batches(list(res_arrow))

    out_path = training_path(base_path, tid_str, build_id)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(out_table, out_path)

    manifest = {
        "build_id": build_id,
        "tenant_id": tid_str,
        "labels_path": str(labels_path),
        "feature_refs": [{"name": fr["name"], "version": fr["version"]} for fr in feature_refs_resolved],
        "spec_hashes": [fr["spec_hash"] for fr in feature_refs_resolved],
        "row_count": out_table.num_rows,
    }
    manifest_path = training_manifest_path(base_path, tid_str, build_id)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    conn.close()
    return (out_path, manifest_path, manifest)
