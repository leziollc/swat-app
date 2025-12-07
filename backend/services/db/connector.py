"""
Database connector for Databricks SQL.

This module provides functions to connect to Databricks SQL warehouses
and execute queries against Unity Catalog tables.
"""

from functools import lru_cache
from typing import Any

import pandas as pd
from databricks import sql
from databricks.sdk.core import Config

# Lazily load Databricks SDK Config to avoid import-time auth errors
_cfg: Config | None = None


def _get_config() -> Config | None:
    global _cfg
    if _cfg is not None:
        return _cfg
    try:
        _cfg = Config()
    except Exception:
        _cfg = None
    return _cfg


@lru_cache(maxsize=1)
def get_connection(warehouse_id: str):
    """
    Get or create a connection to the Databricks SQL warehouse.
    Connection is cached using lru_cache to avoid creating multiple connections.

    Args:
        warehouse_id: The ID of the SQL warehouse to connect to

    Returns:
        A connection to the SQL warehouse
    """
    cfg = _get_config()

    http_path = f"/sql/1.0/warehouses/{warehouse_id}"

    # If the Databricks SDK Config is not available (e.g. in unit tests or
    # local dev without environment variables), still call `sql.connect`
    # so tests that mock `services.db.connector.sql` can observe the call.
    if cfg is None:
        # Provide a placeholder server_hostname so typed stubs are satisfied.
        # In tests `services.db.connector.sql` is typically mocked, so these
        # values are unused; this keeps mypy happy while allowing runtime
        # mocking in CI/tests.
        return sql.connect(server_hostname="", http_path=http_path)

    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )


def close_connections():
    """
    Close all open connections.
    This should be called when shutting down the application.
    """
    # Clear the lru_cache to close connections
    get_connection.cache_clear()


def query(
    sql_query: str, warehouse_id: str, as_dict: bool = True, params: list[Any] | None = None
) -> list[dict] | pd.DataFrame:
    """
    Execute a query against a Databricks SQL Warehouse.

    Args:
        sql_query: SQL query to execute
        warehouse_id: The ID of the SQL warehouse to connect to
        as_dict: Whether to return results as dictionaries (True) or pandas DataFrame (False)

    Returns:
        Query results as a list of dictionaries or pandas DataFrame

    Raises:
        Exception: If the query fails
    """
    conn = get_connection(warehouse_id)

    try:
        with conn.cursor() as cursor:
            if params:
                cursor.execute(sql_query, params)
            else:
                cursor.execute(sql_query)

            # Use fetchall directly for non-Arrow results
            # and convert to appropriate format
            result = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            if as_dict:
                # Convert to list of dictionaries
                return [dict(zip(columns, row)) for row in result]
            else:
                # Convert to pandas DataFrame
                return pd.DataFrame(result, columns=columns)

    except Exception as e:
        # Don't close the cached connection on error
        raise Exception(f"Query failed: {str(e)}")


def insert_data(table_path: str, data: list[dict], warehouse_id: str) -> int:
    """
    Insert data into a Databricks Unity Catalog table.

    Args:
        table_path: Full path to the table (catalog.schema.table)
        data: List of dictionaries containing the records to insert
        warehouse_id: The ID of the SQL warehouse to connect to

    Returns:
        Number of records inserted

    Raises:
        Exception: If the insert operation fails
    """
    if not data:
        return 0

    conn = get_connection(warehouse_id)

    try:
        with conn.cursor() as cursor:
            # Get column names from the first record
            columns = list(data[0].keys())
            columns_str = ", ".join(columns)

            # Create placeholders for a single row
            placeholders = ", ".join(["?"] * len(columns))

            # Build the INSERT statement with multiple VALUES clauses
            values_clauses: list[str] = []
            all_values: list[Any] = []

            for record in data:
                values_clauses.append(f"({placeholders})")
                all_values.extend(record[col] for col in columns)

            insert_query = f"""
                INSERT INTO {table_path} ({columns_str})
                VALUES {", ".join(values_clauses)}
            """

            # Execute the insert with all values in a single statement
            cursor.execute(insert_query, all_values)

            # Get the number of affected rows
            return cursor.rowcount

    except Exception as e:
        raise Exception(f"Failed to insert data: {str(e)}")
