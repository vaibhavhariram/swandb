"""Tests for feature registry endpoints."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def _mock_auth_for_registry():
    """Mock auth for registry tests."""
    with patch("services.api.auth.get_async_session") as mock_gs:
        from contextlib import asynccontextmanager

        mock_sess = MagicMock()
        mock_sess.commit = AsyncMock(return_value=None)

        @asynccontextmanager
        async def _cm():
            yield mock_sess

        mock_factory = MagicMock()
        mock_factory.return_value = _cm()
        mock_gs.return_value = mock_factory
        yield


@pytest.fixture
def tenant_id() -> str:
    return "11111111-1111-1111-1111-111111111111"


@patch("services.api.auth.verify_api_key", new_callable=AsyncMock)
@patch("services.api.registry.create_feature", new_callable=AsyncMock)
def test_create_feature(
    mock_create: AsyncMock,
    mock_verify: AsyncMock,
    client: TestClient,
    tenant_id: str,
) -> None:
    """Create feature returns 200 with feature data."""
    mock_verify.return_value = (tenant_id, ["read"])
    mock_feature = MagicMock()
    mock_feature.id = "feat-123"
    mock_feature.name = "my_feature"
    mock_feature.tenant_id = tenant_id
    mock_feature.source_id = None
    mock_feature.created_at = "2025-03-04T12:00:00Z"
    mock_create.return_value = mock_feature

    from chronosdb.db.base import get_session
    from services.api.main import app

    async def _mock_session():
        sess = MagicMock()
        sess.commit = AsyncMock(return_value=None)
        yield sess

    app.dependency_overrides[get_session] = _mock_session
    try:
        response = client.post(
            f"/v1/{tenant_id}/registry/features",
            headers={"Authorization": f"Bearer key"},
            json={"name": "my_feature"},
        )
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "my_feature"
    assert data["tenant_id"] == tenant_id


@patch("services.api.auth.verify_api_key", new_callable=AsyncMock)
@patch("services.api.registry.create_feature_version", new_callable=AsyncMock)
@patch("services.api.registry.get_feature", new_callable=AsyncMock)
def test_create_version(
    mock_get_feature: AsyncMock,
    mock_create_version: AsyncMock,
    mock_verify: AsyncMock,
    client: TestClient,
    tenant_id: str,
) -> None:
    """Create feature version returns 200 with version data."""
    mock_verify.return_value = (tenant_id, ["read"])
    mock_feature = MagicMock()
    mock_feature.id = "feat-123"
    mock_get_feature.return_value = mock_feature

    mock_fv = MagicMock()
    mock_fv.id = "fv-1"
    mock_fv.version = 1
    mock_fv.transform_spec = {"type": "sql", "query": "SELECT 1"}
    mock_fv.spec_hash = "abc123"
    mock_fv.created_at = "2025-03-04T12:00:00Z"
    mock_create_version.return_value = mock_fv

    from chronosdb.db.base import get_session
    from services.api.main import app

    async def _mock_session():
        sess = MagicMock()
        sess.commit = AsyncMock(return_value=None)
        yield sess

    app.dependency_overrides[get_session] = _mock_session
    try:
        response = client.post(
            f"/v1/{tenant_id}/registry/features/my_feature/versions",
            headers={"Authorization": f"Bearer key"},
            json={"transform_spec": {"type": "sql", "query": "SELECT 1"}},
        )
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 200
    data = response.json()
    assert data["version"] == 1
    assert data["transform_spec"] == {"type": "sql", "query": "SELECT 1"}
    assert data["spec_hash"] == "abc123"


def test_spec_hash_stable_for_equivalent_json() -> None:
    """spec_hash is stable for equivalent JSON (key order doesn't matter)."""
    from chronosdb.transforms.spec import compute_spec_hash

    spec1 = {"a": 1, "b": 2}
    spec2 = {"b": 2, "a": 1}
    assert compute_spec_hash(spec1) == compute_spec_hash(spec2)


def test_spec_hash_different_for_different_json() -> None:
    """spec_hash differs for different JSON."""
    from chronosdb.transforms.spec import compute_spec_hash

    spec1 = {"a": 1}
    spec2 = {"a": 2}
    assert compute_spec_hash(spec1) != compute_spec_hash(spec2)
