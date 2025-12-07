"""Tests for database connection failure scenarios."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from backend.app import app
from backend.services.db.connector import close_connections, insert_data, query


class TestConnectionFailures:
    """Test suite for database connection failure handling."""

    def test_query_with_connection_timeout(self, mocker):
        """Test query behavior when connection times out."""
        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = TimeoutError("Connection timeout")

        mocker.patch(
            "backend.services.db.connector.get_connection",
            return_value=mock_conn
        )

        with pytest.raises(Exception, match="Query failed"):
            query("SELECT * FROM test", "warehouse-123")

    def test_query_with_network_error(self, mocker):
        """Test query behavior when network fails."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = ConnectionError("Network unreachable")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        mocker.patch(
            "backend.services.db.connector.get_connection",
            return_value=mock_conn
        )

        with pytest.raises(Exception, match="Query failed"):
            query("SELECT * FROM test", "warehouse-123")

    def test_query_with_authentication_failure(self, mocker):
        """Test query behavior when authentication fails."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = PermissionError("Authentication failed")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        mocker.patch(
            "backend.services.db.connector.get_connection",
            return_value=mock_conn
        )

        with pytest.raises(Exception, match="Query failed"):
            query("SELECT * FROM test", "warehouse-123")

    def test_insert_with_connection_failure(self, mocker):
        """Test insert operation when connection fails."""
        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = ConnectionError("Connection lost")

        mocker.patch(
            "backend.services.db.connector.get_connection",
            return_value=mock_conn
        )

        with pytest.raises(Exception, match="Failed to insert data"):
            insert_data("catalog.schema.table", [{"id": 1}], "warehouse-123")

    def test_endpoint_handles_database_error_gracefully(self, mocker):
        """Test API endpoint handles database errors with proper error response."""
        mocker.patch(
            "backend.services.db.connector.query",
            side_effect=Exception("Database unavailable")
        )

        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "test",
                    "limit": 10
                }
            )

            assert resp.status_code == 500
            data = resp.json()
            assert data["error"] is True
            assert "Failed to query records table" in data["message"]

    def test_missing_warehouse_id_configuration(self):
        """Test behavior when warehouse ID is not configured."""
        with TestClient(app) as client:
            with patch("backend.config.settings.settings.databricks_warehouse_id", None):
                resp = client.get(
                    "/api/v1/records/read",
                    params={
                        "catalog": "test",
                        "schema": "test",
                        "table": "test",
                        "limit": 10
                    }
                )

                assert resp.status_code == 500
                data = resp.json()
                assert "SQL warehouse ID not configured" in data["message"]

    def test_connection_recovery_after_failure(self, mocker):
        """Test that system can recover after connection failure."""
        call_count = 0

        def side_effect_connection(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("First connection failed")
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = [(1, "test")]
            mock_cursor.description = [("id",), ("name",)]
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            return mock_conn

        mocker.patch(
            "backend.services.db.connector.sql.connect",
            side_effect=side_effect_connection
        )

        with pytest.raises((RuntimeError, ConnectionError)):
            query("SELECT * FROM test", "warehouse-123")

        close_connections()

        result = query("SELECT * FROM test", "warehouse-456", as_dict=True)
        assert len(result) == 1

    def test_concurrent_connection_failures(self, mocker):
        """Test behavior when multiple connections fail simultaneously."""
        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = ConnectionError("Connection pool exhausted")

        mocker.patch(
            "backend.services.db.connector.get_connection",
            return_value=mock_conn
        )

        with TestClient(app) as client:
            responses = []
            for _ in range(3):
                resp = client.get(
                    "/api/v1/records/read",
                    params={
                        "catalog": "test",
                        "schema": "test",
                        "table": "test",
                        "limit": 10
                    }
                )
                responses.append(resp)

            for resp in responses:
                assert resp.status_code == 500

    def test_query_with_corrupted_result(self, mocker):
        """Test handling of corrupted query results."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = None
        mock_cursor.description = [("id",)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        mocker.patch(
            "backend.services.db.connector.get_connection",
            return_value=mock_conn
        )

        with pytest.raises(Exception, match="Query failed"):
            query("SELECT * FROM test", "warehouse-123")

    def test_insert_with_constraint_violation(self, mocker):
        """Test insert when database constraint is violated."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = ValueError("Unique constraint violation")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        mocker.patch(
            "backend.services.db.connector.get_connection",
            return_value=mock_conn
        )

        with pytest.raises(Exception, match="Failed to insert data"):
            insert_data("test.test.test", [{"id": 1, "name": "test"}], "warehouse-123")

    def test_connection_close_failure(self, mocker):
        """Test that connection close failures don't crash the app."""
        mock_conn = MagicMock()
        mock_conn.close.side_effect = Exception("Connection already closed")

        try:
            close_connections()
        except Exception as e:
            pytest.fail(f"close_connections should not raise: {e}")

    def test_warehouse_not_found(self, mocker):
        """Test behavior when specified warehouse doesn't exist."""
        mocker.patch(
            "backend.services.db.connector.sql.connect",
            side_effect=ValueError("Warehouse not found: invalid-warehouse")
        )

        with pytest.raises(ValueError):
            query("SELECT 1", "invalid-warehouse")

    def test_sql_syntax_error_in_query(self, mocker):
        """Test handling of SQL syntax errors."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = SyntaxError("SQL syntax error near 'SELCT'")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        mocker.patch(
            "backend.services.db.connector.get_connection",
            return_value=mock_conn
        )

        with pytest.raises(Exception, match="Query failed"):
            query("SELCT * FROM test", "warehouse-123")

    def test_records_endpoint_connection_failure(self, mocker):
        """Test records endpoint handles connection failures."""
        mocker.patch(
            "backend.services.db.connector.query",
            side_effect=ConnectionError("Database unavailable")
        )

        with TestClient(app) as client:
            resp = client.get(
                "/api/v1/records/read",
                params={
                    "catalog": "test",
                    "schema": "test",
                    "table": "records",
                    "limit": 10
                }
            )

            assert resp.status_code == 500
            assert "Failed to query records table" in resp.json()["message"]


