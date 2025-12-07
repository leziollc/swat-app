"""Tests for the healthcheck endpoint."""

import re
from datetime import datetime

import pytest
from fastapi import status


class TestHealthcheckEndpoint:
    """Test suite for the healthcheck endpoint."""

    def test_healthcheck_status_code(self, client, mocker, monkeypatch):
        """Test the healthcheck endpoint returns 200 OK."""
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "test-warehouse-id")

        mock_query = mocker.patch("backend.services.db.connector.query")
        mock_query.return_value = [[1]]

        response = client.get("/api/v1/healthcheck")
        assert response.status_code == status.HTTP_200_OK

    def test_healthcheck_content(self, client, mocker, monkeypatch):
        """Test the healthcheck endpoint returns the expected content."""
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "test-warehouse-id")

        mock_query = mocker.patch("backend.services.db.connector.query")
        mock_query.return_value = [[1]]

        response = client.get("/api/v1/healthcheck")
        data = response.json()

        assert "status" in data
        assert "components" in data
        assert "timestamp" in data
        assert data["status"] == "healthy"
        assert data["components"]["api"] == "up"
        assert data["components"]["database"] == "connected"

        timestamp = data["timestamp"]
        assert timestamp is not None

        iso_pattern = (
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$"
        )
        assert re.match(
            iso_pattern, timestamp
        ), f"Timestamp '{timestamp}' is not in ISO format"

        try:
            datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            pytest.fail(f"Could not parse timestamp: {timestamp}")

    @pytest.mark.parametrize(
        "accept_header", ["application/json", "application/json; charset=utf-8"]
    )
    def test_healthcheck_content_type(self, client, mocker, monkeypatch, accept_header):
        """Test the healthcheck endpoint handles different accept headers."""
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "test-warehouse-id")

        mock_query = mocker.patch("backend.services.db.connector.query")
        mock_query.return_value = [[1]]

        response = client.get("/api/v1/healthcheck", headers={"Accept": accept_header})
        assert response.status_code == status.HTTP_200_OK
        assert "application/json" in response.headers["content-type"]

    def test_healthcheck_database_connection_success(self, client, mocker, monkeypatch):
        """Test healthcheck when database connection succeeds."""
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "test-warehouse-id")

        mock_query = mocker.patch("backend.services.db.connector.query")
        mock_query.return_value = [[1]]

        response = client.get("/api/v1/healthcheck")
        data = response.json()

        assert response.status_code == status.HTTP_200_OK
        assert data["status"] == "healthy"
        assert "components" in data
        assert data["components"]["api"] == "up"
        assert data["components"]["database"] == "connected"
        mock_query.assert_called_once()

    def test_healthcheck_database_connection_failure(self, client, mocker, monkeypatch):
        """Test healthcheck when database connection fails."""
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "test-warehouse-id")

        mock_query = mocker.patch("backend.services.db.connector.query")
        mock_query.side_effect = Exception("Connection timeout")

        response = client.get("/api/v1/healthcheck")
        data = response.json()

        assert response.status_code == status.HTTP_200_OK
        assert data["status"] == "degraded"
        assert "components" in data
        assert data["components"]["api"] == "up"
        assert "error" in data["components"]["database"]
        assert "Connection timeout" in data["components"]["database"]

    def test_healthcheck_missing_warehouse_id(self, client, mocker, monkeypatch):
        """Test healthcheck when DATABRICKS_WAREHOUSE_ID is not set."""
        monkeypatch.delenv("DATABRICKS_WAREHOUSE_ID", raising=False)

        response = client.get("/api/v1/healthcheck")
        data = response.json()

        assert response.status_code == status.HTTP_200_OK
        assert data["status"] == "degraded"
        assert "components" in data
        assert data["components"]["api"] == "up"
        assert "DATABRICKS_WAREHOUSE_ID not set" in data["components"]["database"]
