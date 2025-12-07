"""Tests for database logging functionality."""


import pytest

from backend.services.logger import DatabaseLogger, db_logger


class TestDatabaseLogger:
    """Test suite for database logging."""

    @pytest.fixture
    def mock_db(self, monkeypatch):
        """Mock database operations."""
        self.logged_entries = []
        self.queries_executed = []

        def fake_query(sql_query, warehouse_id, params=None, as_dict=True):
            self.queries_executed.append(sql_query)
            if "SELECT 1 FROM" in sql_query:
                return [{"col": 1}]
            return []

        def fake_insert(table_path, data, warehouse_id):
            self.logged_entries.extend(data)
            return len(data)

        monkeypatch.setattr("backend.services.db.connector.query", fake_query)
        monkeypatch.setattr("backend.services.db.connector.insert_data", fake_insert)

        monkeypatch.setenv("DATABRICKS_LOGGING_ENABLED", "true")
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "test-warehouse")
        monkeypatch.setenv("DATABRICKS_CATALOG", "test_catalog")
        monkeypatch.setenv("DATABRICKS_SCHEMA", "test_schema")
        monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "test_user")

    def test_logger_initialization(self, mock_db):
        """Test logger initializes with correct configuration."""
        logger = DatabaseLogger()
        assert logger.enabled is True
        assert logger.catalog == "test_catalog"
        assert logger.schema == "test_schema"
        assert logger.table == "api_log"
        assert logger._get_warehouse_id() == "test-warehouse"
        assert logger.user == "test_user"

    def test_logger_disabled_when_flag_false(self, monkeypatch):
        """Test logger is disabled when DATABRICKS_LOGGING_ENABLED is false."""
        monkeypatch.setenv("DATABRICKS_LOGGING_ENABLED", "false")
        logger = DatabaseLogger()
        assert logger.enabled is False

    def test_get_table_path(self, mock_db):
        """Test table path generation."""
        logger = DatabaseLogger()
        table_path = logger._get_table_path()
        assert table_path == "test_catalog.test_schema.api_log"

    def test_get_table_path_returns_none_without_catalog(self, monkeypatch):
        """Test table path returns None if catalog is missing."""
        monkeypatch.delenv("DATABRICKS_CATALOG", raising=False)
        logger = DatabaseLogger()
        table_path = logger._get_table_path()
        assert table_path is None

    def test_ensure_log_table_creates_table(self, mock_db, monkeypatch):
        """Test that log table is created if it doesn't exist."""
        def fake_query_fail_then_succeed(sql_query, warehouse_id, params=None, as_dict=True):
            if "SELECT 1 FROM" in sql_query:
                if not hasattr(fake_query_fail_then_succeed, "called"):
                    fake_query_fail_then_succeed.called = True
                    raise Exception("Table does not exist")
            self.queries_executed.append(sql_query)
            return []

        monkeypatch.setattr("backend.services.db.connector.query", fake_query_fail_then_succeed)

        logger = DatabaseLogger()
        result = logger._ensure_log_table_exists()

        assert result is True
        assert any("CREATE TABLE IF NOT EXISTS" in q for q in self.queries_executed)

    def test_log_error_with_exception(self, mock_db):
        """Test logging an error with exception details."""
        logger = DatabaseLogger()

        try:
            raise ValueError("Test error message")
        except ValueError as e:
            logger.log_error(e, request=None, level="ERROR")

        assert len(self.logged_entries) == 1
        entry = self.logged_entries[0]

        assert entry["level"] == "ERROR"
        assert entry["error_type"] == "ValueError"
        assert entry["error_message"] == "Test error message"
        assert entry["user"] == "test_user"
        assert "log_id" in entry
        assert "timestamp" in entry
        assert "stack_trace" in entry

    def test_log_error_with_request_context(self, mock_db, monkeypatch):
        """Test logging includes request context."""
        from unittest.mock import Mock

        mock_request = Mock()
        mock_request.url.path = "/api/v1/records"
        mock_request.method = "POST"

        logger = DatabaseLogger()

        try:
            raise ValueError("Test error with request")
        except ValueError as e:
            logger.log_error(e, request=mock_request)

        assert len(self.logged_entries) == 1
        entry = self.logged_entries[0]
        assert entry["endpoint"] == "/api/v1/records"
        assert entry["method"] == "POST"

    def test_log_error_with_additional_context(self, mock_db):
        """Test logging includes additional context."""
        logger = DatabaseLogger()

        try:
            raise RuntimeError("Database connection failed")
        except RuntimeError as e:
            logger.log_error(
                e,
                request=None,
                level="ERROR",
                additional_context={
                    "catalog": "my_catalog",
                    "schema": "my_schema",
                    "table": "my_table",
                    "status_code": 500,
                },
            )

        assert len(self.logged_entries) == 1
        entry = self.logged_entries[0]

        assert entry["catalog"] == "my_catalog"
        assert entry["schema"] == "my_schema"
        assert entry["table_name"] == "my_table"
        assert entry["status_code"] == 500

    def test_log_error_does_nothing_when_disabled(self, monkeypatch, mock_db):
        """Test that logging is skipped when disabled."""
        monkeypatch.setenv("DATABRICKS_LOGGING_ENABLED", "false")
        logger = DatabaseLogger()

        try:
            raise ValueError("This should not be logged")
        except ValueError as e:
            logger.log_error(e, request=None)

        assert len(self.logged_entries) == 0

    def test_log_error_handles_logging_failure_gracefully(self, mock_db, monkeypatch):
        """Test that logging failures don't break the application."""
        def fake_insert_fail(table_path, data, warehouse_id):
            raise Exception("Database insert failed")

        monkeypatch.setattr("backend.services.db.connector.insert_data", fake_insert_fail)

        logger = DatabaseLogger()

        try:
            raise ValueError("Original error")
        except ValueError as e:
            logger.log_error(e, request=None)

    def test_log_event_creates_log_entry(self, mock_db):
        """Test logging a general event."""
        logger = DatabaseLogger()
        logger.log_event(
            "User action completed successfully",
            request=None,
            level="INFO",
            additional_context={"catalog": "test_cat"},
        )

        assert len(self.logged_entries) == 1
        entry = self.logged_entries[0]

        assert entry["level"] == "INFO"
        assert "User action completed successfully" in entry["error_message"]
        assert entry["catalog"] == "test_cat"

    def test_global_logger_instance_exists(self):
        """Test that global db_logger instance is available."""
        assert db_logger is not None
        assert isinstance(db_logger, DatabaseLogger)
