"""Test configuration for the FastAPI application."""

import pytest
from fastapi.testclient import TestClient

from backend.app import app


@pytest.fixture(scope="session")
def app_instance():
    """Create an application instance for testing."""
    return app


@pytest.fixture
def client(app_instance):
    """Create a test client for the FastAPI application."""
    with TestClient(app_instance) as test_client:
        yield test_client