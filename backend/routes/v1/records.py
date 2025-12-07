import json
import os
import re
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends

from ...config.settings import get_settings
from ...errors.exceptions import ConfigurationError, DatabaseError, ValidationError
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
        desc = db_connector.query(f"DESCRIBE {table_path}", warehouse_id=warehouse_id, as_dict=True)
        if isinstance(desc, list):
            rows = desc
        else:
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

    column_defs = []
    for col in schema_definition:
        nullable_clause = "" if col.nullable else " NOT NULL"
        column_defs.append(f"{col.name} {col.data_type}{nullable_clause}")

    column_defs.extend([
        "record_uuid STRING",
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
    """Validate that data conforms to the schema definition.

    Raises: \n
        ValueError: If validation fails. The exception includes the expected schema
                   information in its string representation for error responses.
    """

    schema_map = {col.name: col.data_type for col in schema_definition}
    required_cols = {col.name for col in schema_definition if not col.nullable}

    audit_columns = {"record_uuid", "is_deleted", "inserted_at", "inserted_by", "updated_at", "updated_by", "deleted_at", "deleted_by"}

    expected_schema = [
        {
            "name": col.name,
            "type": col.data_type,
            "nullable": col.nullable
        }
        for col in schema_definition
    ]

    for idx, record in enumerate(data):
        missing_cols = required_cols - set(record.keys())
        if missing_cols:
            error_msg = f"Record {idx} missing required columns: {missing_cols}"
            raise ValueError(f"{error_msg}||SCHEMA||{expected_schema}")

        unknown_cols = set(record.keys()) - set(schema_map.keys()) - audit_columns
        if unknown_cols:
            error_msg = f"Record {idx} contains unknown columns: {unknown_cols}"
            raise ValueError(f"{error_msg}||SCHEMA||{expected_schema}")

        for col_name, col_value in record.items():
            if col_name in schema_map and col_value is not None:
                expected_type = schema_map[col_name]
                try:
                    _validate_value_type(col_name, col_value, expected_type, idx)
                except ValueError as e:
                    raise ValueError(f"{str(e)}||SCHEMA||{expected_schema}") from e


def _validate_value_type(col_name: str, value: Any, expected_type: str, record_idx: int) -> None:
    """Validate that a value matches the expected SQL type."""
    type_upper = expected_type.upper()

    if type_upper in ("BIGINT", "INT", "INTEGER", "SMALLINT", "TINYINT"):
        if not isinstance(value, int):
            raise ValueError(f"Record {record_idx}: Column '{col_name}' expects integer, got {type(value).__name__}")

    elif type_upper in ("DOUBLE", "FLOAT", "DECIMAL"):
        if not isinstance(value, (int, float)):
            raise ValueError(f"Record {record_idx}: Column '{col_name}' expects numeric, got {type(value).__name__}")

    elif type_upper in ("STRING", "VARCHAR", "CHAR"):
        if not isinstance(value, str):
            raise ValueError(f"Record {record_idx}: Column '{col_name}' expects string, got {type(value).__name__}")

    elif type_upper == "BOOLEAN":
        if not isinstance(value, bool):
            raise ValueError(f"Record {record_idx}: Column '{col_name}' expects boolean, got {type(value).__name__}")

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
    Read data from a records table in Unity Catalog with optional filtering and pagination.

    This endpoint retrieves records data with support for:
    - Column selection
    - Pagination via limit and offset
    - JSON-structured filters
    - Automatic exclusion of soft-deleted records (if is_deleted column exists)

    Args: \n
        catalog: The catalog name containing the records table
        schema: The schema name containing the records table
        table: The records table name
        limit: Maximum number of records to return (default: 100)
        offset: Number of records to skip for pagination (default: 0)
        columns: Comma-separated list of columns to retrieve (default: "*")
        filters: Optional JSON string with structured filters, e.g., '[{"column": "status", "op": "=", "value": "completed"}]'
        settings: Application settings (injected)

    Returns: \n
        TableResponse containing the retrieved records data and count

    Raises: \n
        ConfigurationError: If the SQL warehouse ID is not configured
        DatabaseError: If identifier validation fails or the query operation fails
    """
    warehouse_id = settings.databricks_warehouse_id
    if not warehouse_id:
        raise ConfigurationError(message="SQL warehouse ID not configured")

    try:
        _validate_identifier(catalog)
        _validate_identifier(schema)
        _validate_identifier(table)
    except ValueError as ve:
        raise ValidationError(message=str(ve)) from ve

    params = TableQueryParams(
        catalog=catalog, schema=schema, table=table, limit=limit, offset=offset, columns=columns, filters=None
    )

    table_path = _table_path(params.catalog, params.schema_name, params.table)

    has_is_deleted = _has_column(table_path, "is_deleted", warehouse_id)

    user_filters: list[dict[str, Any]] | None = None
    if filters:
        try:
            parsed = json.loads(filters)
            if isinstance(parsed, list):
                user_filters = parsed
        except Exception as e:
            raise DatabaseError(message="Invalid filters JSON payload") from e

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

        import pandas as _pd

        if isinstance(results, _pd.DataFrame):
            data_list = results.to_dict(orient="records")
        else:
            data_list = results

        return TableResponse(data=data_list, count=len(data_list), total=None)
    except Exception as e:
        raise DatabaseError(message=f"Failed to query records table: {e}") from e


@router.post("/write", response_model=TableResponse, status_code=201)
async def write_records(request: TableInsertRequest, settings=Depends(get_settings)):
    """
    Insert new records data into a Unity Catalog table.

    This endpoint automatically adds audit fields to each record:
    - record_uuid: Unique identifier for the record (auto-generated if not provided)
    - inserted_at: Timestamp of record creation (if not provided)
    - inserted_by: User or system that created the record (if not provided)
    - updated_at: Timestamp of last update (initialized to insert time)
    - updated_by: User or system that last updated the record (initialized to inserter)
    - is_deleted: Soft delete flag (initialized to false)
    - deleted_at: Timestamp of deletion (initialized to null)
    - deleted_by: User who deleted the record (initialized to null)

    If auto_create=true, the endpoint will:
    1. Create the table with the provided schema_definition and required audit columns if it doesn't exist
    2. Validate data against the schema definition before inserting

    Note: Catalog and schema must already exist in Databricks (they are not auto-created).

    Args: \n
        request: The request containing:
            - catalog: Catalog name (must already exist)
            - schema_name: Schema name (must already exist)
            - table: Table name
            - data: Records to insert
            - auto_create: Whether to auto-create table (default: False)
            - schema_definition: Required when auto_create=true, defines table columns
        settings: Application settings (injected)

    Returns: \n
        TableResponse containing the inserted records data and count of records inserted

    Raises: \n
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
        raise ValidationError(message=str(ve)) from ve

    table_path = _table_path(request.catalog, request.schema_name, request.table)

    if request.auto_create:
        try:
            _create_catalog_if_not_exists(request.catalog, warehouse_id)

            _create_schema_if_not_exists(request.catalog, request.schema_name, warehouse_id)

            if not _table_exists(table_path, warehouse_id):
                if not request.schema_definition:
                    raise DatabaseError(message="schema_definition is required when creating a new table")
                _create_table_from_schema(table_path, request.schema_definition, warehouse_id)

        except DatabaseError:
            raise
        except Exception as e:
            raise DatabaseError(message=f"Failed to auto-create resources: {e}") from e

    if request.schema_definition:
        try:
            _validate_data_against_schema(request.data, request.schema_definition)
        except DatabaseError:
            raise
        except Exception as e:
            error_str = str(e)
            error_details = {}

            if "||SCHEMA||" in error_str:
                error_msg, schema_part = error_str.split("||SCHEMA||", 1)
                try:
                    import ast
                    error_details["expected_schema"] = ast.literal_eval(schema_part)
                except Exception:
                    pass
            else:
                error_msg = error_str

            raise ValidationError(
                message=f"Schema validation failed: {error_msg}",
                details=error_details if error_details else None
            ) from e

    inserted_by = os.getenv("DATABRICKS_USER") or os.getenv("DATABRICKS_CONFIG_PROFILE") or "api"
    now = datetime.now(UTC).isoformat()
    rows = []
    for r in request.data:
        rec = dict(r)
        if "record_uuid" not in rec:
            from uuid import uuid4
            rec["record_uuid"] = str(uuid4())
        if "inserted_at" not in rec:
            rec["inserted_at"] = now
        if "inserted_by" not in rec:
            rec["inserted_by"] = inserted_by
        if "updated_at" not in rec:
            rec["updated_at"] = now
        if "updated_by" not in rec:
            rec["updated_by"] = inserted_by
        if "is_deleted" not in rec:
            rec["is_deleted"] = False
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
        raise DatabaseError(message=f"Failed to insert into records table: {e}") from e


@router.put("/update", response_model=TableResponse)
async def update_records(request: TableUpdateRequest, settings=Depends(get_settings)):
    """
    Update one or more records in a Unity Catalog table.

    Supports 4 update scenarios:
    1. Single record: Provide key_column, key_value, and updates
    2. Multiple records (same updates): Provide key_column, key_values (array), and updates
    3. Multiple records (different updates): Provide key_column and bulk_updates (array of {key_value, updates})
    4. Filter-based: Provide filters and updates

    Automatically updates audit metadata:
    - updated_at: Timestamp of the update
    - updated_by: User or system performing the update

    Note: record_uuid is not modified during updates (remains the original value from insert)

    Args: \n
        request: The request containing table info and update method
        settings: Application settings (injected)

    Returns: \n
        TableResponse containing count of updated records

    Raises: \n
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
        if request.key_column:
            _validate_identifier(request.key_column)
    except ValueError as ve:
        raise DatabaseError(message=str(ve)) from ve

    catalog = request.catalog or os.getenv("DATABRICKS_CATALOG")
    schema = request.schema_name or os.getenv("DATABRICKS_SCHEMA")
    table_path = _table_path(catalog, schema, request.table)

    now = datetime.now(UTC).isoformat()
    updated_by = os.getenv("DATABRICKS_USER") or os.getenv("DATABRICKS_CONFIG_PROFILE") or "api"

    try:
        total_updated = 0
        not_found = []

        if request.key_value is not None:
            check_query = f"SELECT COUNT(*) as count FROM {table_path} WHERE {request.key_column} = ?"
            check_result = db_connector.query(check_query, warehouse_id=warehouse_id, params=[request.key_value], as_dict=True)
            if check_result and check_result[0].get("count", 0) > 0:
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

                db_connector.query(sql_query, warehouse_id=warehouse_id, params=params)
                total_updated = 1
            else:
                total_updated = 0
                not_found.append(request.key_value)

        elif request.key_values is not None:
            placeholders_check = ", ".join(["?"] * len(request.key_values))
            check_query = f"SELECT {request.key_column} FROM {table_path} WHERE {request.key_column} IN ({placeholders_check})"
            existing_records = db_connector.query(check_query, warehouse_id=warehouse_id, params=list(request.key_values), as_dict=True)

            existing_keys = [record[request.key_column] for record in existing_records] if existing_records else []
            existing_count = len(existing_keys)

            requested_keys = set(request.key_values)
            found_keys = set(existing_keys)
            not_found = list(requested_keys - found_keys)

            if existing_count > 0:
                updates = dict(request.updates)
                updates["updated_at"] = now
                updates["updated_by"] = updated_by

                set_clauses = []
                params_multi: list[Any] = []
                for k, v in updates.items():
                    _validate_identifier(k)
                    set_clauses.append(f"{k} = ?")
                    params_multi.append(v)

                placeholders = ", ".join(["?"] * len(request.key_values))
                params_multi.extend(request.key_values)

                set_sql = ", ".join(set_clauses)
                sql_query = f"UPDATE {table_path} SET {set_sql} WHERE {request.key_column} IN ({placeholders})"

                db_connector.query(sql_query, warehouse_id=warehouse_id, params=params_multi)
                total_updated = existing_count
            else:
                total_updated = 0
                not_found = list(request.key_values)

        elif request.bulk_updates is not None:
            for item in request.bulk_updates:
                check_query = f"SELECT COUNT(*) as count FROM {table_path} WHERE {request.key_column} = ?"
                check_result = db_connector.query(check_query, warehouse_id=warehouse_id, params=[item.key_value], as_dict=True)
                if check_result and check_result[0].get("count", 0) > 0:
                    updates = dict(item.updates)
                    updates["updated_at"] = now
                    updates["updated_by"] = updated_by

                    set_clauses = []
                    params_bulk: list[Any] = []
                    for k, v in updates.items():
                        _validate_identifier(k)
                        set_clauses.append(f"{k} = ?")
                        params_bulk.append(v)

                    params_bulk.append(item.key_value)
                    set_sql = ", ".join(set_clauses)
                    sql_query = f"UPDATE {table_path} SET {set_sql} WHERE {request.key_column} = ?"

                    db_connector.query(sql_query, warehouse_id=warehouse_id, params=params_bulk)
                    total_updated += 1
                else:
                    not_found.append(item.key_value)

        elif request.filters is not None:
            updates = dict(request.updates)
            updates["updated_at"] = now
            updates["updated_by"] = updated_by

            set_clauses = []
            params_filter: list[Any] = []
            for k, v in updates.items():
                _validate_identifier(k)
                set_clauses.append(f"{k} = ?")
                params_filter.append(v)

            where_clause, filter_params = build_where_clause(request.filters)
            params_filter.extend(filter_params)

            set_sql = ", ".join(set_clauses)
            sql_query = f"UPDATE {table_path} SET {set_sql} WHERE {where_clause}"

            db_connector.query(sql_query, warehouse_id=warehouse_id, params=params_filter)
            total_updated = -1

        response_data: dict[str, Any] = {"updated_at": now, "updated_by": updated_by}
        if not_found:
            response_data["not_found"] = not_found

        return TableResponse(
            data=[response_data],
            count=total_updated,
            total=None
        )
    except Exception as e:
        raise DatabaseError(message=f"Failed to update records table: {e}") from e


@router.delete("/delete", response_model=TableResponse)
async def delete_records(request: TableDeleteRequest, settings=Depends(get_settings)):
    """
    Delete one or more records from a Unity Catalog table.

    Supports 3 deletion scenarios:
    1. Single record: Provide key_column, key_value, and soft
    2. Multiple records: Provide key_column, key_values (array), and soft
    3. Filter-based: Provide filters and soft

    Supports both soft delete and hard delete operations:
    - Soft delete (soft=True): If table has 'is_deleted' column, marks record(s) as deleted
    - Hard delete (soft=False): Permanently removes the record(s) from the table

    Soft delete automatically updates:
    - is_deleted: Set to true
    - deleted_at: Timestamp of deletion
    - deleted_by: User or system performing deletion
    - updated_at: Timestamp of deletion
    - updated_by: User or system performing deletion

    Note: record_uuid is not modified during soft deletes (remains the original value from insert)

    Args: \n
        request: The request containing table info and deletion method
        settings: Application settings (injected)

    Returns: \n
        TableResponse with deletion confirmation and count

    Raises: \n
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
        if request.key_column:
            _validate_identifier(request.key_column)
    except ValueError as ve:
        raise DatabaseError(message=str(ve)) from ve

    catalog = request.catalog or os.getenv("DATABRICKS_CATALOG")
    schema = request.schema_name or os.getenv("DATABRICKS_SCHEMA")
    table_path = _table_path(catalog, schema, request.table)

    if request.soft:
        has_is_deleted = _has_column(table_path, "is_deleted", warehouse_id)
        if not has_is_deleted:
            raise DatabaseError(
                message=f"Soft delete requested but table {table_path} does not have 'is_deleted' column. "
                        "Use soft=false for hard delete or add 'is_deleted' column to the table."
            )

    try:
        total_deleted = 0
        not_found_keys = []
        now = datetime.now(UTC).isoformat()
        deleted_by = os.getenv("DATABRICKS_USER") or os.getenv("DATABRICKS_CONFIG_PROFILE") or "api"

        if request.key_value is not None:
            check_query = f"SELECT COUNT(*) as cnt FROM {table_path} WHERE {request.key_column} = ?"
            count_result = db_connector.query(check_query, warehouse_id=warehouse_id, params=[request.key_value], as_dict=True)
            record_exists = count_result[0]["cnt"] > 0 if count_result else False

            if not record_exists:
                not_found_keys = [request.key_value]
                total_deleted = 0
            else:
                if request.soft:
                    sql_query = f"UPDATE {table_path} SET is_deleted = true, deleted_at = ?, deleted_by = ?, updated_at = ?, updated_by = ? WHERE {request.key_column} = ?"
                    params = [now, deleted_by, now, deleted_by, request.key_value]
                else:
                    # Hard delete single record
                    sql_query = f"DELETE FROM {table_path} WHERE {request.key_column} = ?"
                    params = [request.key_value]

                db_connector.query(sql_query, warehouse_id=warehouse_id, params=params)
                total_deleted = 1

        elif request.key_values is not None:
            placeholders = ", ".join(["?"] * len(request.key_values))

            check_query = f"SELECT {request.key_column} FROM {table_path} WHERE {request.key_column} IN ({placeholders})"
            existing_records = db_connector.query(check_query, warehouse_id=warehouse_id, params=list(request.key_values), as_dict=True)

            existing_keys = set()
            if existing_records:
                for record in existing_records:
                    existing_keys.add(record[request.key_column])

            requested_keys = set(request.key_values)
            not_found_keys = list(requested_keys - existing_keys)

            if request.soft:
                sql_query = f"UPDATE {table_path} SET is_deleted = true, deleted_at = ?, deleted_by = ?, updated_at = ?, updated_by = ? WHERE {request.key_column} IN ({placeholders})"
                params_multi = [now, deleted_by, now, deleted_by] + list(request.key_values)
            else:
                sql_query = f"DELETE FROM {table_path} WHERE {request.key_column} IN ({placeholders})"
                params_multi = list(request.key_values)

            db_connector.query(sql_query, warehouse_id=warehouse_id, params=params_multi)
            total_deleted = len(existing_keys)

        elif request.filters is not None:
            where_clause, filter_params = build_where_clause(request.filters)

            if request.soft:
                sql_query = f"UPDATE {table_path} SET is_deleted = true, deleted_at = ?, deleted_by = ?, updated_at = ?, updated_by = ? WHERE {where_clause}"
                params_filter = [now, deleted_by, now, deleted_by] + filter_params
            else:
                sql_query = f"DELETE FROM {table_path} WHERE {where_clause}"
                params_filter = filter_params

            db_connector.query(sql_query, warehouse_id=warehouse_id, params=params_filter)
            total_deleted = -1

        response_data: list[dict[str, Any]] = [{"is_deleted": True}] if request.soft else []

        if not_found_keys:
            response_data.append({"not_found": not_found_keys})

        return TableResponse(data=response_data, count=total_deleted, total=None)

    except DatabaseError:
        raise
    except Exception as e:
        raise DatabaseError(message=f"Failed to delete record(s): {e}") from e

