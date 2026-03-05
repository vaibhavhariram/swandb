"""Pytest fixtures."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock

from services.api.main import app


@pytest.fixture
def client() -> TestClient:
    """FastAPI test client."""
    return TestClient(app)
