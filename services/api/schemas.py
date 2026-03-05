"""Pydantic schemas for API request/response."""

from datetime import datetime
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
