"""Tests for the database connector module using pytest best practices."""

import pandas as pd
import pytest

from backend.services.db.connector import close_connections, get_connection, insert_data, query


@pytest.fixture
def mock_sql(mocker):
    """Mock the databricks sql module."""
    return mocker.patch("backend.services.db.connector.sql")


@pytest.fixture
def mock_cursor(mocker):
    """Create a mock cursor with test data."""
    cursor = mocker.MagicMock()
    cursor.description = [("id",), ("name",)]
    cursor.fetchall.return_value = [(1, "Test")]

    def execute_side_effect(query, *args, **kwargs):
        if "INSERT" in query.upper():
            value_count = query.count("(?, ?)")
            cursor.rowcount = value_count
        elif "SELECT" in query.upper():
            cursor.rowcount = len(cursor.fetchall.return_value)
        else:
            cursor.rowcount = 1
        return None

    cursor.execute.side_effect = execute_side_effect

    cursor.rowcount = 0
    return cursor


@pytest.fixture
def mock_connection(mocker, mock_cursor):
    """Create a mock database connection."""
    conn = mocker.MagicMock()
    conn.cursor.return_value.__enter__.return_value = mock_cursor
    return conn


class TestDatabaseConnector:
    """Tests for the database connector functionality."""

    def test_get_connection_creates_proper_connection(self, mocker, mock_sql):
        """Test that get_connection creates a connection with the correct parameters."""
        # Arrange
        warehouse_id = "test-warehouse-id"
        expected_http_path = f"/sql/1.0/warehouses/{warehouse_id}"

        get_connection(warehouse_id)

        mock_sql.connect.assert_called_once()
        call_kwargs = mock_sql.connect.call_args.kwargs
        assert call_kwargs["http_path"] == expected_http_path

    def test_query_returns_dict_results(self, mocker, mock_connection, mock_cursor):
        """Test that query returns results as dictionaries when as_dict=True."""
        test_query = "SELECT * FROM catalog.schema.table"
        mocker.patch(
            "backend.services.db.connector.get_connection", return_value=mock_connection
        )
        result = query(test_query, "warehouse-id", as_dict=True)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["name"] == "Test"
        mock_cursor.execute.assert_called_once_with(test_query)

    def test_query_returns_dataframe(self, mocker, mock_connection, mock_cursor):
        """Test that query returns results as a DataFrame when as_dict=False."""
        test_query = "SELECT * FROM catalog.schema.table"
        mocker.patch(
            "backend.services.db.connector.get_connection", return_value=mock_connection
        )
        result = query(test_query, "warehouse-id", as_dict=False)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["id"] == 1
        assert result.iloc[0]["name"] == "Test"
        mock_cursor.execute.assert_called_once_with(test_query)

    def test_query_handles_exceptions(self, mocker):
        """Test that query properly handles and wraps exceptions."""
        mock_conn = mocker.MagicMock()
        mock_conn.cursor.return_value.__enter__.side_effect = ValueError(
            "Database connection error"
        )
        mocker.patch("backend.services.db.connector.get_connection", return_value=mock_conn)

        with pytest.raises(Exception) as exc_info:
            query("SELECT 1", "warehouse-id")

        assert "Query failed" in str(exc_info.value)
        assert "Database connection error" in str(exc_info.value)

    def test_close_connections_clears_cache(self, mocker):
        """Test that close_connections clears the connection cache."""
        mock_cache_clear = mocker.patch.object(get_connection, "cache_clear")

        close_connections()

        mock_cache_clear.assert_called_once()


class TestInsertData:
    """Test suite for insert_data function."""

    def test_insert_data_success(self, mocker, mock_connection):
        """Test successful data insertion."""
        test_data = [{"id": 1, "name": "Test1"}, {"id": 2, "name": "Test2"}]

        mocker.patch(
            "backend.services.db.connector.get_connection", return_value=mock_connection
        )

        result = insert_data(
            table_path="test_catalog.test_schema.test_table",
            data=test_data,
            warehouse_id="test-warehouse-123",
        )

        assert result == 2

        mock_cursor = mock_connection.cursor.return_value.__enter__.return_value
        mock_cursor.execute.assert_called_once()

        call_args = mock_cursor.execute.call_args[0]
        sql = call_args[0]
        params = call_args[1]

        assert "INSERT INTO test_catalog.test_schema.test_table" in sql
        assert "(id, name)" in sql
        assert "VALUES (?, ?), (?, ?)" in sql

        assert params == [1, "Test1", 2, "Test2"]

    def test_insert_data_empty(self, mocker, mock_connection):
        """Test insertion with empty data list."""
        mocker.patch(
            "backend.services.db.connector.get_connection", return_value=mock_connection
        )

        result = insert_data(
            table_path="test_catalog.test_schema.test_table",
            data=[],
            warehouse_id="test-warehouse-123",
        )

        assert result == 0
        mock_connection.cursor.assert_not_called()

    def test_insert_data_error(self, mocker, mock_connection):
        """Test error handling during insertion."""
        mock_cursor = mock_connection.cursor.return_value.__enter__.return_value
        mock_cursor.execute.side_effect = Exception("Database error")

        mocker.patch(
            "backend.services.db.connector.get_connection", return_value=mock_connection
        )

        with pytest.raises(Exception) as exc_info:
            insert_data(
                table_path="test_catalog.test_schema.test_table",
                data=[{"id": 1, "name": "Test"}],
                warehouse_id="test-warehouse-123",
            )

        assert "Failed to insert data" in str(exc_info.value)

    def test_insert_data_different_columns(self, mocker, mock_connection):
        """Test insertion with records having different columns."""
        test_data = [
            {"id": 1, "name": "Test1"},
            {"id": 2, "description": "Test2"},
        ]

        mocker.patch(
            "backend.services.db.connector.get_connection", return_value=mock_connection
        )

        with pytest.raises(Exception) as exc_info:
            insert_data(
                table_path="test_catalog.test_schema.test_table",
                data=test_data,
                warehouse_id="test-warehouse-123",
            )

        assert "Failed to insert data" in str(exc_info.value)
