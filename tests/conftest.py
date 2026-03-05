"""Pytest fixtures."""

import pytest
from fastapi.testclient import TestClient

from services.api.main import app


@pytest.fixture
def client() -> TestClient:
    """FastAPI test client."""
    return TestClient(app)
