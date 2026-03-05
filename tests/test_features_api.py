"""Tests for features/get API."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def _mock_auth():
    with patch("services.api.auth.get_async_session") as mock_gs:
        from contextlib import asynccontextmanager
        mock_sess = MagicMock()
        mock_sess.commit = AsyncMock()

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
@patch("services.api.features.aioredis")
def test_features_get_returns_values_and_feature_ts(
    mock_aioredis: MagicMock,
    mock_verify: AsyncMock,
    client: TestClient,
    tenant_id: str,
) -> None:
    """/features/get returns expected values and feature_ts."""
    mock_verify.return_value = (tenant_id, ["read"])
    mock_client = AsyncMock()
    mock_client.mget = AsyncMock(side_effect=[
        [b'{"value": "100", "feature_ts": "2025-03-05T10:37:00"}'],
        [b'{"value": "100", "feature_ts": "2025-03-05T10:37:00"}'],
    ])
    mock_client.aclose = AsyncMock()
    mock_aioredis.from_url.return_value = mock_client

    response = client.post(
            f"/v1/{tenant_id}/features/get",
            headers={"Authorization": "Bearer key"},
            json={
                "entity_keys": {"user_id": "u1"},
                "feature_refs": [{"name": "amount", "version": 1}],
                "as_of_ts": "2025-03-05T10:37:00Z",
            },
        )

    assert response.status_code == 200
    data = response.json()
    assert "features" in data
    assert len(data["features"]) == 1
    assert data["features"][0]["value"] == "100"
    assert data["features"][0]["feature_ts"] == "2025-03-05T10:37:00"
    assert data["features"][0]["status"] == "ok"
