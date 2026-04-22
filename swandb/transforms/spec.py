"""Transform spec hashing for immutability."""

import hashlib
import json
from typing import Any


def canonical_json(obj: dict[str, Any]) -> str:
    """Serialize dict to canonical JSON (sorted keys, no extra whitespace)."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def compute_spec_hash(transform_spec: dict[str, Any]) -> str:
    """Compute SHA-256 hash of canonical JSON of transform_spec."""
    canonical = canonical_json(transform_spec)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
