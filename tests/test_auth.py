"""Tests for API key auth and tenant-scoped endpoints."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def tenant_id() -> str:
    return "11111111-1111-1111-1111-111111111111"


@pytest.fixture(autouse=True)
def _mock_db_session():
    """Mock get_async_session so auth tests don't need a real DB."""
    mock_session = MagicMock()
    mock_session.commit = AsyncMock(return_value=None)

    @asynccontextmanager
    async def _session_cm():
        yield mock_session

    mock_factory = MagicMock()
    mock_factory.return_value = _session_cm()
    with patch("services.api.auth.get_async_session", return_value=mock_factory):
        yield


@patch("services.api.auth.verify_api_key", new_callable=AsyncMock)
def test_v1_healthz_missing_key_returns_401(
    mock_verify: AsyncMock,
    client: TestClient,
    tenant_id: str,
) -> None:
    """Missing Authorization header returns 401."""
    response = client.get(f"/v1/{tenant_id}/healthz")
    assert response.status_code == 401
    mock_verify.assert_not_called()
    detail = response.json()["detail"]
    assert "missing" in detail.lower() or "invalid" in detail.lower() or "authorization" in detail.lower()


@patch("services.api.auth.verify_api_key", new_callable=AsyncMock)
def test_v1_healthz_wrong_key_returns_401(
    mock_verify: AsyncMock,
    client: TestClient,
    tenant_id: str,
) -> None:
    """Invalid API key returns 401."""
    mock_verify.return_value = None

    response = client.get(
        f"/v1/{tenant_id}/healthz",
        headers={"Authorization": "Bearer wrong-key-xyz"},
    )
    assert response.status_code == 401
    detail = response.json()["detail"]
    assert "invalid" in detail.lower() or "key" in detail.lower()


@patch("services.api.auth.verify_api_key", new_callable=AsyncMock)
def test_v1_healthz_correct_key_returns_200(
    mock_verify: AsyncMock,
    client: TestClient,
    tenant_id: str,
) -> None:
    """Valid API key returns 200 on tenant-scoped healthz."""
    mock_verify.return_value = (tenant_id, ["read"])

    response = client.get(
        f"/v1/{tenant_id}/healthz",
        headers={"Authorization": "Bearer valid-api-key"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["tenant_id"] == tenant_id
