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

    if cfg is None:
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

            result = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            if as_dict:
                return [dict(zip(columns, row, strict=False)) for row in result]
            else:
                return pd.DataFrame(result, columns=columns)

    except Exception as e:
        raise Exception(f"Query failed: {str(e)}") from e


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
            columns = list(data[0].keys())
            columns_str = ", ".join(columns)

            placeholders = ", ".join(["?"] * len(columns))

            values_clauses: list[str] = []
            all_values: list[Any] = []

            for record in data:
                values_clauses.append(f"({placeholders})")
                all_values.extend(record[col] for col in columns)

            insert_query = f"""
                INSERT INTO {table_path} ({columns_str})
                VALUES {", ".join(values_clauses)}
            """

            cursor.execute(insert_query, all_values)

            return int(cursor.rowcount)

    except Exception as e:
        raise Exception(f"Failed to insert data: {str(e)}") from e
