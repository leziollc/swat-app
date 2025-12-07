import json
import os
import re
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends

from ...config.settings import get_settings
from ...errors.exceptions import ConfigurationError, DatabaseError
from ...models.tables import (
    TableDeleteRequest,
    TableInsertRequest,
    TableQueryParams,
    TableResponse,
    TableUpdateRequest,
)
from ...services.db import connector as db_connector
from ...services.db.sql_helpers import build_where_clause

router = APIRouter(tags=["records"])


IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str) -> None:
    if not IDENT_RE.match(name):
        raise ValueError(f"Invalid identifier: {name}")


def _table_path(catalog: str | None, schema: str | None, table: str) -> str:
    if not catalog or not schema:
        raise ValueError("Catalog and schema must be provided")
    return f"{catalog}.{schema}.{table}"


def _has_column(table_path: str, column: str, warehouse_id: str) -> bool:
    try:
        # DESCRIBE returns rows describing columns; search for column name
        desc = db_connector.query(f"DESCRIBE {table_path}", warehouse_id=warehouse_id, as_dict=True)
        # Normalize to list of dicts
        if isinstance(desc, list):
            rows = desc
        else:
            # If a DataFrame was returned, coerce
            try:
                import pandas as _pd

                rows = _pd.DataFrame(desc).to_dict(orient="records")
            except Exception:
                rows = []

        for row in rows:
            # Different dialects may name the column differently
            if any(column == v for v in row.values()):
                return True
        return False
    except Exception:
        return False


def _catalog_exists(catalog: str, warehouse_id: str) -> bool:
    """Check if a catalog exists."""
    try:
        db_connector.query(f"SHOW CATALOGS LIKE '{catalog}'", warehouse_id=warehouse_id)
        return True
    except Exception:
        return False


def _schema_exists(catalog: str, schema: str, warehouse_id: str) -> bool:
    """Check if a schema exists."""
    try:
        db_connector.query(f"SHOW SCHEMAS IN {catalog} LIKE '{schema}'", warehouse_id=warehouse_id)
        return True
    except Exception:
        return False


def _table_exists(table_path: str, warehouse_id: str) -> bool:
    """Check if a table exists."""
    try:
        db_connector.query(f"DESCRIBE TABLE {table_path}", warehouse_id=warehouse_id)
        return True
    except Exception:
        return False


def _create_catalog_if_not_exists(catalog: str, warehouse_id: str) -> None:
    """Create catalog if it doesn't exist."""
    if not _catalog_exists(catalog, warehouse_id):
        db_connector.query(f"CREATE CATALOG IF NOT EXISTS {catalog}", warehouse_id=warehouse_id)


def _create_schema_if_not_exists(catalog: str, schema: str, warehouse_id: str) -> None:
    """Create schema if it doesn't exist."""
    if not _schema_exists(catalog, schema, warehouse_id):
        db_connector.query(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}", warehouse_id=warehouse_id)


def _create_table_from_schema(table_path: str, schema_definition: list, warehouse_id: str) -> None:
    """Create a table with the given schema definition and required audit columns."""

    # Build column definitions from user schema
    column_defs = []
    for col in schema_definition:
        nullable_clause = "" if col.nullable else " NOT NULL"
        column_defs.append(f"{col.name} {col.data_type}{nullable_clause}")

    # Add required audit columns (always included)
    column_defs.extend([
        "is_deleted BOOLEAN",
        "inserted_at TIMESTAMP",
        "inserted_by STRING",
        "updated_at TIMESTAMP",
        "updated_by STRING",
        "deleted_at TIMESTAMP",
        "deleted_by STRING"
    ])

    columns_sql = ", ".join(column_defs)
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_path} ({columns_sql})"

    db_connector.query(create_table_sql, warehouse_id=warehouse_id)


def _validate_data_against_schema(data: list[dict], schema_definition: list) -> None:
    """Validate that data conforms to the schema definition."""

    # Build a map of column names to their types
    schema_map = {col.name: col.data_type for col in schema_definition}
    required_cols = {col.name for col in schema_definition if not col.nullable}

    # Audit columns that are automatically added and should be excluded from validation
    audit_columns = {"is_deleted", "inserted_at", "inserted_by", "updated_at", "updated_by", "deleted_at", "deleted_by"}

    for idx, record in enumerate(data):
        # Check for required columns
        missing_cols = required_cols - set(record.keys())
        if missing_cols:
            raise ValueError(f"Record {idx} missing required columns: {missing_cols}")

        # Check for unknown columns (exclude audit columns)
        unknown_cols = set(record.keys()) - set(schema_map.keys()) - audit_columns
        if unknown_cols:
            raise ValueError(f"Record {idx} contains unknown columns: {unknown_cols}")

        # Validate data types (basic validation)
        for col_name, col_value in record.items():
            if col_name in schema_map and col_value is not None:
                expected_type = schema_map[col_name]
                _validate_value_type(col_name, col_value, expected_type, idx)


def _validate_value_type(col_name: str, value: Any, expected_type: str, record_idx: int) -> None:
    """Validate that a value matches the expected SQL type."""
    type_upper = expected_type.upper()

    # Integer types
    if type_upper in ("BIGINT", "INT", "INTEGER", "SMALLINT", "TINYINT"):
        if not isinstance(value, int):
            raise ValueError(f"Record {record_idx}: Column '{col_name}' expects integer, got {type(value).__name__}")

    # Float types
    elif type_upper in ("DOUBLE", "FLOAT", "DECIMAL"):
        if not isinstance(value, (int, float)):
            raise ValueError(f"Record {record_idx}: Column '{col_name}' expects numeric, got {type(value).__name__}")

    # String types
    elif type_upper in ("STRING", "VARCHAR", "CHAR"):
        if not isinstance(value, str):
            raise ValueError(f"Record {record_idx}: Column '{col_name}' expects string, got {type(value).__name__}")

    # Boolean
    elif type_upper == "BOOLEAN":
        if not isinstance(value, bool):
            raise ValueError(f"Record {record_idx}: Column '{col_name}' expects boolean, got {type(value).__name__}")

    # Date/Timestamp (accept strings or date/datetime objects)
    elif type_upper in ("DATE", "TIMESTAMP"):
        if not isinstance(value, (str, datetime)):
            raise ValueError(f"Record {record_idx}: Column '{col_name}' expects date/timestamp string, got {type(value).__name__}")


@router.get("/read", response_model=TableResponse)
async def read_records(
    catalog: str,
    schema: str,
    table: str,
    limit: int = 100,
    offset: int = 0,
    columns: str = "*",
    filters: str | None = None,
    settings=Depends(get_settings),
):
    """
    Read data from an records table in Unity Catalog with optional filtering and pagination.

    This endpoint retrieves records data with support for:
    - Column selection
    - Pagination via limit and offset
    - JSON-structured filters
    - Automatic exclusion of soft-deleted records (if is_deleted column exists)

    Args:
        catalog: The catalog name containing the records table
        schema: The schema name containing the records table
        table: The records table name
        limit: Maximum number of records to return (default: 100)
        offset: Number of records to skip for pagination (default: 0)
        columns: Comma-separated list of columns to retrieve (default: "*")
        filters: Optional JSON string with structured filters, e.g., '[{"column": "status", "op": "=", "value": "completed"}]'
        settings: Application settings (injected)

    Returns:
        TableResponse containing the retrieved records data and count

    Raises:
        ConfigurationError: If the SQL warehouse ID is not configured
        DatabaseError: If identifier validation fails or the query operation fails
    """
    # Validate settings
    warehouse_id = settings.databricks_warehouse_id
    if not warehouse_id:
        raise ConfigurationError(message="SQL warehouse ID not configured")

    # Validate identifiers
    try:
        _validate_identifier(catalog)
        _validate_identifier(schema)
        _validate_identifier(table)
    except ValueError as ve:
        raise DatabaseError(message=str(ve))

    params = TableQueryParams(
        catalog=catalog, schema=schema, table=table, limit=limit, offset=offset, columns=columns, filters=None
    )

    table_path = _table_path(params.catalog, params.schema_name, params.table)

    # If table has is_deleted column, by default exclude deleted rows
    has_is_deleted = _has_column(table_path, "is_deleted", warehouse_id)

    # Parse client filters (JSON string) if provided
    user_filters: list[dict[str, Any]] | None = None
    if filters:
        try:
            parsed = json.loads(filters)
            if isinstance(parsed, list):
                user_filters = parsed
        except Exception:
            raise DatabaseError(message="Invalid filters JSON payload")

    # If table has is_deleted column, append exclusion to filters
    if has_is_deleted:
        deleted_filter = {"column": "is_deleted", "op": "=", "value": False}
        if user_filters:
            user_filters.append(deleted_filter)
        else:
            user_filters = [deleted_filter]

    where_clause, params_list = build_where_clause(user_filters)

    sql_query = f"SELECT {params.columns} FROM {table_path} {where_clause} LIMIT {params.limit} OFFSET {params.offset}"

    try:
        results = db_connector.query(sql_query, warehouse_id=warehouse_id, params=params_list, as_dict=True)

        # Normalize results to list[dict]
        import pandas as _pd

        if isinstance(results, _pd.DataFrame):
            data_list = results.to_dict(orient="records")
        else:
            data_list = results

        return TableResponse(data=data_list, count=len(data_list), total=None)
    except Exception as e:
        raise DatabaseError(message=f"Failed to query records table: {e}")


@router.post("/write", response_model=TableResponse)
async def write_records(request: TableInsertRequest, settings=Depends(get_settings)):
    """
    Insert new records data into a Unity Catalog table.

    This endpoint automatically adds audit fields to each record:
    - inserted_at: Timestamp of record creation (if not provided)
    - inserted_by: User or system that created the record (if not provided)
    - updated_at: Timestamp of last update (initialized to insert time)
    - updated_by: User or system that last updated the record (initialized to inserter)
    - is_deleted: Soft delete flag (initialized to false)
    - deleted_at: Timestamp of deletion (initialized to null)
    - deleted_by: User who deleted the record (initialized to null)

    If auto_create=true, the endpoint will:
    1. Create the catalog if it doesn't exist
    2. Create the schema if it doesn't exist
    3. Create the table with the provided schema_definition and required audit columns if it doesn't exist
    4. Validate data against the schema definition before inserting

    Args:
        request: The request containing:
            - catalog: Catalog name
            - schema_name: Schema name
            - table: Table name
            - data: Records to insert
            - auto_create: Whether to auto-create catalog/schema/table (default: False)
            - schema_definition: Required when auto_create=true, defines table columns
        settings: Application settings (injected)

    Returns:
        TableResponse containing the inserted records data and count of records inserted

    Raises:
        ConfigurationError: If the SQL warehouse ID is not configured
        DatabaseError: If identifier validation fails, schema validation fails, or insert operation fails
    """
    warehouse_id = settings.databricks_warehouse_id
    if not warehouse_id:
        raise ConfigurationError(message="SQL warehouse ID not configured")

    try:
        _validate_identifier(request.catalog)
        _validate_identifier(request.schema_name)
        _validate_identifier(request.table)
    except ValueError as ve:
        raise DatabaseError(message=str(ve))

    table_path = _table_path(request.catalog, request.schema_name, request.table)

    # Handle auto-creation of catalog, schema, and table
    if request.auto_create:
        try:
            # Create catalog if it doesn't exist
            _create_catalog_if_not_exists(request.catalog, warehouse_id)

            # Create schema if it doesn't exist
            _create_schema_if_not_exists(request.catalog, request.schema_name, warehouse_id)

            # Create table if it doesn't exist
            if not _table_exists(table_path, warehouse_id):
                if not request.schema_definition:
                    raise DatabaseError(message="schema_definition is required when creating a new table")
                _create_table_from_schema(table_path, request.schema_definition, warehouse_id)

            # Validate data against schema if schema_definition is provided
            if request.schema_definition:
                _validate_data_against_schema(request.data, request.schema_definition)

        except DatabaseError:
            raise
        except Exception as e:
            raise DatabaseError(message=f"Failed to auto-create resources: {e}")

    # Add audit fields if not present
    inserted_by = os.getenv("DATABRICKS_USER") or os.getenv("DATABRICKS_CONFIG_PROFILE") or "api"
    now = datetime.now(datetime.UTC).isoformat() if hasattr(datetime, 'UTC') else datetime.utcnow().isoformat()
    rows = []
    for r in request.data:
        rec = dict(r)
        # Add insert audit fields
        if "inserted_at" not in rec:
            rec["inserted_at"] = now
        if "inserted_by" not in rec:
            rec["inserted_by"] = inserted_by
        # Initialize update audit fields (same as insert on creation)
        if "updated_at" not in rec:
            rec["updated_at"] = now
        if "updated_by" not in rec:
            rec["updated_by"] = inserted_by
        # Initialize soft delete flag
        if "is_deleted" not in rec:
            rec["is_deleted"] = False
        # Initialize deletion audit fields (null until deleted)
        if "deleted_at" not in rec:
            rec["deleted_at"] = None
        if "deleted_by" not in rec:
            rec["deleted_by"] = None
        rows.append(rec)

    try:
        inserted = db_connector.insert_data(table_path=table_path, data=rows, warehouse_id=warehouse_id)
        if inserted < 0:
            inserted = len(rows)
        return TableResponse(data=rows, count=inserted, total=inserted)
    except Exception as e:
        raise DatabaseError(message=f"Failed to insert into records table: {e}")


@router.put("/update", response_model=TableResponse)
async def update_records(request: TableUpdateRequest, settings=Depends(get_settings)):
    """
    Update an existing record in a Unity Catalog table.

    This endpoint updates a single record identified by a key column and value.
    Automatically adds audit metadata:
    - updated_at: Timestamp of the update
    - updated_by: User or system performing the update

    Args:
        request: The request containing:
            - catalog: The catalog name (optional, can use environment default)
            - schema_name: The schema name (optional, can use environment default)
            - table: The table name
            - key_column: The column name to identify the record (e.g., "record_id")
            - key_value: The value of the key column for the record to update
            - updates: Dictionary of column names and new values
        settings: Application settings (injected)

    Returns:
        TableResponse containing the updated fields and confirmation

    Raises:
        ConfigurationError: If the SQL warehouse ID is not configured
        DatabaseError: If identifier validation fails or the update operation fails
    """
    warehouse_id = settings.databricks_warehouse_id
    if not warehouse_id:
        raise ConfigurationError(message="SQL warehouse ID not configured")

    try:
        _validate_identifier(request.catalog or "default_catalog")
        _validate_identifier(request.schema_name or "default_schema")
        _validate_identifier(request.table)
        _validate_identifier(request.key_column)
    except ValueError as ve:
        raise DatabaseError(message=str(ve))

    catalog = request.catalog or os.getenv("DATABRICKS_CATALOG")
    schema = request.schema_name or os.getenv("DATABRICKS_SCHEMA")
    table_path = _table_path(catalog, schema, request.table)

    # Add updated metadata
    now = datetime.now(datetime.UTC).isoformat() if hasattr(datetime, 'UTC') else datetime.utcnow().isoformat()
    updated_by = os.getenv("DATABRICKS_USER") or os.getenv("DATABRICKS_CONFIG_PROFILE") or "api"
    updates = dict(request.updates)
    updates["updated_at"] = now
    updates["updated_by"] = updated_by

    set_clauses = []
    params: list[Any] = []
    for k, v in updates.items():
        _validate_identifier(k)
        set_clauses.append(f"{k} = ?")
        params.append(v)

    params.append(request.key_value)

    set_sql = ", ".join(set_clauses)
    sql_query = f"UPDATE {table_path} SET {set_sql} WHERE {request.key_column} = ?"

    try:
        db_connector.query(sql_query, warehouse_id=warehouse_id, params=params)
        # Return the updates as confirmation
        return TableResponse(data=[updates], count=1, total=None)
    except Exception as e:
        raise DatabaseError(message=f"Failed to update records table: {e}")


@router.delete("/delete", response_model=TableResponse)
async def delete_records(request: TableDeleteRequest, settings=Depends(get_settings)):
    """
    Delete a record from a Unity Catalog table.

    Supports both soft delete and hard delete operations:
    - Soft delete: If table has 'is_deleted' column and soft=True, marks record as deleted
    - Hard delete: Permanently removes the record from the table

    Soft delete automatically updates:
    - is_deleted: Set to true
    - deleted_at: Timestamp of deletion
    - deleted_by: User or system performing deletion
    - updated_at: Timestamp of deletion
    - updated_by: User or system performing deletion

    Args:
        request: The request containing:
            - catalog: The catalog name (optional, can use environment default)
            - schema_name: The schema name (optional, can use environment default)
            - table: The table name
            - key_column: The column name to identify the record (e.g., "record_id")
            - key_value: The value of the key column for the record to delete
            - soft: Whether to perform soft delete (default: True)
        settings: Application settings (injected)

    Returns:
        TableResponse with deletion confirmation

    Raises:
        ConfigurationError: If the SQL warehouse ID is not configured
        DatabaseError: If identifier validation fails or the delete operation fails
    """
    warehouse_id = settings.databricks_warehouse_id
    if not warehouse_id:
        raise ConfigurationError(message="SQL warehouse ID not configured")

    try:
        _validate_identifier(request.catalog or "default_catalog")
        _validate_identifier(request.schema_name or "default_schema")
        _validate_identifier(request.table)
        _validate_identifier(request.key_column)
    except ValueError as ve:
        raise DatabaseError(message=str(ve))

    catalog = request.catalog or os.getenv("DATABRICKS_CATALOG")
    schema = request.schema_name or os.getenv("DATABRICKS_SCHEMA")
    table_path = _table_path(catalog, schema, request.table)

    # If soft delete requested, check if table has is_deleted column
    if request.soft:
        has_is_deleted = _has_column(table_path, "is_deleted", warehouse_id)
        if not has_is_deleted:
            raise DatabaseError(
                message=f"Soft delete requested but table {table_path} does not have 'is_deleted' column. "
                        "Use soft=false for hard delete or add 'is_deleted' column to the table."
            )

        # Perform soft delete by updating is_deleted flag and deletion audit fields
        now = datetime.now(datetime.UTC).isoformat() if hasattr(datetime, 'UTC') else datetime.utcnow().isoformat()
        deleted_by = os.getenv("DATABRICKS_USER") or os.getenv("DATABRICKS_CONFIG_PROFILE") or "api"
        sql_query = f"UPDATE {table_path} SET is_deleted = true, deleted_at = ?, deleted_by = ?, updated_at = ?, updated_by = ? WHERE {request.key_column} = ?"
        params = [now, deleted_by, now, deleted_by, request.key_value]
        try:
            db_connector.query(sql_query, warehouse_id=warehouse_id, params=params)
            return TableResponse(data=[{"is_deleted": True}], count=1, total=None)
        except Exception as e:
            raise DatabaseError(message=f"Failed to soft-delete record: {e}")

    # Hard delete - permanently remove the record
    try:
        sql_query = f"DELETE FROM {table_path} WHERE {request.key_column} = ?"
        params = [request.key_value]
        db_connector.query(sql_query, warehouse_id=warehouse_id, params=params)
        return TableResponse(data=[], count=1, total=None)
    except Exception as e:
        raise DatabaseError(message=f"Failed to delete record: {e}")

