"""Tests for materialization API endpoint."""

import uuid
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def _mock_auth_and_session():
    with patch("services.api.auth.get_async_session") as mock_gs:
        from contextlib import asynccontextmanager

        mock_sess = MagicMock()
        mock_sess.commit = AsyncMock()
        mock_sess.add = MagicMock()
        mock_sess.flush = AsyncMock()

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
@patch("services.api.materialize.materialize_enqueue")
def test_post_materialize_enqueues_job(
    mock_enqueue: MagicMock,
    mock_verify: AsyncMock,
    client: TestClient,
    tenant_id: str,
) -> None:
    """POST /materialize enqueues job and returns job_id."""
    mock_verify.return_value = (tenant_id, ["read"])

    from swandb.db.base import get_session
    from services.api.main import app

    async def _mock_session():
        sess = MagicMock()
        sess.commit = AsyncMock()
        sess.add = MagicMock()
        sess.flush = AsyncMock()

        def _add(obj):
            # Simulate ORM assigning id on flush
            if hasattr(obj, "id") and not getattr(obj, "id", None):
                obj.id = uuid.uuid4()

        sess.add.side_effect = _add
        yield sess

    app.dependency_overrides[get_session] = _mock_session
    try:
        response = client.post(
            f"/v1/{tenant_id}/materialize",
            headers={"Authorization": "Bearer key"},
            json={
                "range_start": "2025-03-05",
                "range_end": "2025-03-05",
                "feature_refs": [{"name": "amount_feature", "version": 1}],
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert "job_id" in data
    mock_enqueue.assert_called_once()
    payload = mock_enqueue.call_args[0][0]
    assert payload["range_start"] == date(2025, 3, 5)
    assert payload["range_end"] == date(2025, 3, 5)
    assert payload["feature_refs"] == [{"name": "amount_feature", "version": 1}]


@patch("services.api.auth.verify_api_key", new_callable=AsyncMock)
def test_post_materialize_rejects_invalid_range(
    mock_verify: AsyncMock,
    client: TestClient,
    tenant_id: str,
) -> None:
    """POST /materialize rejects range_end < range_start."""
    mock_verify.return_value = (tenant_id, ["read"])

    from swandb.db.base import get_session
    from services.api.main import app

    async def _mock_session():
        yield MagicMock()

    app.dependency_overrides[get_session] = _mock_session
    try:
        response = client.post(
            f"/v1/{tenant_id}/materialize",
            headers={"Authorization": "Bearer key"},
            json={
                "range_start": "2025-03-10",
                "range_end": "2025-03-05",
                "feature_refs": [{"name": "x", "version": 1}],
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 400
    assert "range_end" in response.json().get("detail", "").lower()
