"""Pydantic schemas for API request/response."""

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field


# --- Feature registry ---


class CreateFeatureRequest(BaseModel):
    """Request to create a feature."""

    name: str = Field(..., min_length=1, description="Feature name")
    source_id: str | None = Field(None, description="Optional source ID")


class CreateFeatureResponse(BaseModel):
    """Response after creating a feature."""

    id: str
    name: str
    tenant_id: str
    source_id: str | None
    created_at: datetime


class CreateFeatureVersionRequest(BaseModel):
    """Request to create a feature version."""

    transform_spec: dict[str, Any] = Field(..., description="Transform specification (JSON)")


class FeatureVersionResponse(BaseModel):
    """A single feature version."""

    id: str
    version: int
    transform_spec: dict[str, Any]
    spec_hash: str
    created_at: datetime


class GetFeatureResponse(BaseModel):
    """Feature with its versions."""

    id: str
    name: str
    tenant_id: str
    source_id: str | None
    created_at: datetime
    versions: list[FeatureVersionResponse]


# --- Event ingestion ---


class IngestEvent(BaseModel):
    """Single event in a batch."""

    event_id: str | None = Field(None, description="Optional; else event_hash computed")
    entity_keys: dict[str, Any] | list[Any] = Field(default_factory=dict)
    event_ts: datetime
    event_type: str = ""
    payload: dict[str, Any] | Any = Field(default_factory=dict)


class IngestEventsRequest(BaseModel):
    """Batch event ingestion request."""

    source_id: str = Field(..., description="Source ID")
    idempotency_key: str = Field(..., min_length=1, description="Idempotency key")
    events: list[IngestEvent] = Field(..., min_length=1)


class IngestEventsResponse(BaseModel):
    """Response after ingesting events."""

    job_id: str
    event_count: int
    max_event_ts: datetime | None = None
    max_ingest_ts: datetime | None = None


# --- Materialization ---


class FeatureRef(BaseModel):
    """Reference to a feature and version."""

    name: str = Field(..., min_length=1, description="Feature name")
    version: int = Field(..., ge=1, description="Feature version")


class MaterializeRequest(BaseModel):
    """Request to materialize offline features."""

    range_start: date = Field(..., description="Start date (inclusive)")
    range_end: date = Field(..., description="End date (inclusive)")
    feature_refs: list[FeatureRef] = Field(
        ...,
        min_length=1,
        description="Features to materialize",
    )


class MaterializeResponse(BaseModel):
    """Response after enqueuing materialization job."""

    job_id: str


# --- Feature serving ---


class GetFeaturesRequest(BaseModel):
    """Request to fetch online features."""

    entity_keys: dict[str, str] = Field(
        ...,
        description="Entity identifiers, e.g. {'user_id': 'u1'}",
    )
    feature_refs: list[FeatureRef] = Field(
        ...,
        min_length=1,
        description="Features to fetch",
    )
    as_of_ts: datetime = Field(
        ...,
        description="Point-in-time for feature values",
    )


class FeatureValue(BaseModel):
    """Single feature value in get response."""

    feature_ref: dict[str, Any]
    value: Any = None
    feature_ts: str | None = None
    status: str = Field(
        ...,
        description="ok | current_fallback | not_found | missing_entity | invalid_ref",
    )


class GetFeaturesResponse(BaseModel):
    """Response from online feature fetch."""

    features: list[FeatureValue]


# --- Training build ---


class TrainingBuildRequest(BaseModel):
    """Request to build training dataset."""

    labels_path: str = Field(..., description="Path to labels parquet (entity_id, label_ts, ...)")
    feature_refs: list[FeatureRef] = Field(
        ...,
        min_length=1,
        description="Features to join",
    )
    entity_key: str = Field(default="user_id", description="Entity key in entity_keys")


class TrainingBuildResponse(BaseModel):
    """Response from training build."""

    build_id: str
    output_path: str
    manifest_path: str
    row_count: int
    feature_refs: list[dict[str, Any]]
    spec_hashes: list[str]


# --- Parity validation ---


class ValidateParityRequest(BaseModel):
    """Request for offline/online parity validation."""

    feature_refs: list[FeatureRef] = Field(
        ...,
        min_length=1,
        description="Features to validate",
    )
    entity_keys: list[str] | None = Field(
        default=None,
        description="Entity key names, e.g. ['user_id']. If omitted, derived from registry (first feature's transform_spec.entity_key).",
    )
    sample_size: int = Field(default=100, ge=1, le=10000)
    threshold: float = Field(default=0.0, ge=0.0, le=1.0, description="Max allowed mismatch_rate")


class ValidateParityResponse(BaseModel):
    """Response from parity validation."""

    job_id: str
    status: str
    sample_size: int
    mismatch_count: int
    mismatch_rate: float
    threshold: float
