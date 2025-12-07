"""
Database logging service for API errors and events.

This module provides functionality to log API errors and events to a Databricks table
for persistent audit trails and troubleshooting.
"""

import os
import traceback
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fastapi import Request


class DatabaseLogger:
    """Service for logging API errors and events to Databricks table."""

    def __init__(self):
        """Initialize the database logger."""
        self.enabled = os.getenv("DATABRICKS_LOGGING_ENABLED", "true").lower() == "true"
        self.catalog = os.getenv("DATABRICKS_LOG_CATALOG") or os.getenv("DATABRICKS_CATALOG")
        self.schema = os.getenv("DATABRICKS_LOG_SCHEMA") or os.getenv("DATABRICKS_SCHEMA")
        self.table = "api_log"
        self.user = os.getenv("DATABRICKS_USER") or os.getenv("DATABRICKS_CONFIG_PROFILE") or "api"

    def _get_warehouse_id(self) -> str | None:
        """Get the warehouse ID from environment, reading it dynamically."""
        return os.getenv("DATABRICKS_WAREHOUSE_ID")

    def _get_table_path(self, catalog: str | None = None, schema: str | None = None) -> str | None:
        """Get the full table path for logging."""
        log_catalog = catalog or self.catalog
        log_schema = schema or self.schema

        if not log_catalog or not log_schema:
            return None
        return f"{log_catalog}.{log_schema}.{self.table}"

    def _ensure_log_table_exists(self, catalog: str | None = None, schema: str | None = None) -> bool:
        """
        Ensure the api_log table exists, create if it doesn't.

        Args:
            catalog: Optional catalog name (extracted from request)
            schema: Optional schema name (extracted from request)

        Returns:
            bool: True if table exists or was created, False otherwise
        """
        warehouse_id = self._get_warehouse_id()
        if not self.enabled or not warehouse_id:
            return False

        table_path = self._get_table_path(catalog=catalog, schema=schema)
        if not table_path:
            return False

        try:
            from .db.connector import query

            try:
                query(f"SELECT 1 FROM {table_path} LIMIT 1", warehouse_id=warehouse_id)
                return True
            except Exception:
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_path} (
                    log_id STRING NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    level STRING NOT NULL,
                    endpoint STRING,
                    method STRING,
                    status_code INT,
                    error_type STRING,
                    error_message STRING,
                    stack_trace STRING,
                    request_body STRING,
                    user STRING,
                    catalog STRING,
                    schema STRING,
                    table_name STRING,
                    execution_time_ms DOUBLE
                )
                """
                query(create_table_sql, warehouse_id=warehouse_id)
                return True

        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Failed to ensure log table exists: {e}", exc_info=True)
            return False

    def log_error(
        self,
        error: Exception,
        request: Request | None = None,
        level: str = "ERROR",
        additional_context: dict[str, Any] | None = None,
    ) -> None:
        """
        Log an error to the Databricks table.

        Args:
            error: The exception that occurred
            request: The FastAPI request object (optional)
            level: Log level (ERROR, WARNING, INFO)
            additional_context: Additional context to include in the log
        """
        import logging
        logger = logging.getLogger(__name__)
        warehouse_id = self._get_warehouse_id()
        logger.info(f"log_error called: enabled={self.enabled}, warehouse_id={warehouse_id}")

        if not self.enabled:
            logger.info("Logging disabled, returning")
            return

        try:
            endpoint = request.url.path if request else None
            method = request.method if request else None

            request_body = None
            request_catalog = None
            request_schema = None
            request_table = None

            if request and hasattr(request, "_body"):
                try:
                    body_bytes = request._body
                    if body_bytes:
                        request_body = body_bytes.decode("utf-8")
                        import json
                        try:
                            body_json = json.loads(request_body)
                            request_catalog = body_json.get("catalog")
                            request_schema = body_json.get("schema") or body_json.get("schema_name")
                            request_table = body_json.get("table") or body_json.get("table_name")
                        except Exception:
                            pass

                        if len(request_body) > 5000:
                            request_body = request_body[:5000] + "... (truncated)"
                except Exception:
                    request_body = "<unable to decode>"

            context = additional_context or {}
            catalog = context.get("catalog") or request_catalog
            schema = context.get("schema") or request_schema
            table_name = context.get("table") or request_table

            if not self._ensure_log_table_exists(catalog=catalog, schema=schema):
                return

            table_path = self._get_table_path(catalog=catalog, schema=schema)
            if not table_path:
                return
            status_code = context.get("status_code")
            execution_time = context.get("execution_time_ms")

            log_entry = {
                "log_id": str(uuid4()),
                "timestamp": datetime.now(UTC).isoformat(),
                "level": level,
                "endpoint": endpoint,
                "method": method,
                "status_code": status_code,
                "error_type": type(error).__name__,
                "error_message": str(error),
                "stack_trace": traceback.format_exc(),
                "request_body": request_body,
                "user": self.user,
                "catalog": catalog,
                "schema": schema,
                "table_name": table_name,
                "execution_time_ms": execution_time,
            }

            from .db.connector import insert_data
            insert_data(table_path, [log_entry], warehouse_id=warehouse_id)

        except Exception as log_error:
            import logging
            logging.getLogger(__name__).warning(f"Failed to log error to database: {log_error}")

    def log_event(
        self,
        message: str,
        request: Request | None = None,
        level: str = "INFO",
        additional_context: dict[str, Any] | None = None,
    ) -> None:
        """
        Log a general event to the Databricks table.

        Args:
            message: The event message
            request: The FastAPI request object (optional)
            level: Log level (ERROR, WARNING, INFO)
            additional_context: Additional context to include in the log
        """
        class LogEvent(Exception):
            pass

        self.log_error(
            LogEvent(message),
            request=request,
            level=level,
            additional_context=additional_context,
        )


db_logger = DatabaseLogger()
