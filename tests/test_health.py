"""Tests for health and readiness endpoints."""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient


def test_healthz_returns_200(client: TestClient) -> None:
    """healthz returns 200 when process is running."""
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@patch("services.api.main._check_redis")
@patch("services.api.main._check_postgres")
def test_readyz_returns_200_when_dependencies_up(
    mock_postgres: object,
    mock_redis: object,
    client: TestClient,
) -> None:
    """readyz returns 200 when Postgres and Redis are reachable."""
    mock_postgres.return_value = True
    mock_redis.return_value = True

    response = client.get("/readyz")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["postgres"] == "ok"
    assert data["redis"] == "ok"
