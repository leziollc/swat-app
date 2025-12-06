from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends

from ...config.settings import get_settings
from ...errors.exceptions import ConfigurationError, DatabaseError
from ...models.tables import (
    TableQueryParams,
    TableResponse,
    TableInsertRequest,
    TableUpdateRequest,
    TableDeleteRequest,
)
from ...services.db import connector as db_connector
from ...services.db.sql_helpers import build_where_clause
import json

import re
from datetime import datetime
import os

router = APIRouter(tags=["orders"])


IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str) -> None:
    if not IDENT_RE.match(name):
        raise ValueError(f"Invalid identifier: {name}")


def _table_path(catalog: Optional[str], schema: Optional[str], table: str) -> str:
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


@router.get("/read", response_model=TableResponse)
async def read_orders(
    catalog: str,
    schema: str,
    table: str,
    limit: int = 100,
    offset: int = 0,
    columns: str = "*",
    filters: Optional[str] = None,
    settings=Depends(get_settings),
):
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
    user_filters: Optional[List[Dict[str, Any]]] = None
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
        raise DatabaseError(message=f"Failed to query orders table: {e}")


@router.post("/write", response_model=TableResponse)
async def write_orders(request: TableInsertRequest, settings=Depends(get_settings)):
    warehouse_id = settings.databricks_warehouse_id
    if not warehouse_id:
        raise ConfigurationError(message="SQL warehouse ID not configured")

    try:
        _validate_identifier(request.catalog)
        _validate_identifier(request.schema_name)
        _validate_identifier(request.table)
    except ValueError as ve:
        raise DatabaseError(message=str(ve))

    # Add audit fields if not present
    created_by = os.getenv("DATABRICKS_USER") or os.getenv("DATABRICKS_CONFIG_PROFILE") or "api"
    now = datetime.utcnow().isoformat()
    rows = []
    for r in request.data:
        rec = dict(r)
        if "created_at" not in rec:
            rec["created_at"] = now
        if "created_by" not in rec:
            rec["created_by"] = created_by
        rows.append(rec)

    table_path = _table_path(request.catalog, request.schema_name, request.table)

    try:
        inserted = db_connector.insert_data(table_path=table_path, data=rows, warehouse_id=warehouse_id)
        if inserted < 0:
            inserted = len(rows)
        return TableResponse(data=rows, count=inserted, total=inserted)
    except Exception as e:
        raise DatabaseError(message=f"Failed to insert into orders table: {e}")


@router.put("/update", response_model=TableResponse)
async def update_orders(request: TableUpdateRequest, settings=Depends(get_settings)):
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
    now = datetime.utcnow().isoformat()
    updated_by = os.getenv("DATABRICKS_USER") or os.getenv("DATABRICKS_CONFIG_PROFILE") or "api"
    updates = dict(request.updates)
    updates["updated_at"] = now
    updates["updated_by"] = updated_by

    set_clauses = []
    params: List[Any] = []
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
        raise DatabaseError(message=f"Failed to update orders table: {e}")


@router.delete("/delete", response_model=TableResponse)
async def delete_orders(request: TableDeleteRequest, settings=Depends(get_settings)):
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

    # If soft delete requested and table has is_deleted, perform an update
    if request.soft:
        has_is_deleted = _has_column(table_path, "is_deleted", warehouse_id)
        if has_is_deleted:
            now = datetime.utcnow().isoformat()
            updated_by = os.getenv("DATABRICKS_USER") or os.getenv("DATABRICKS_CONFIG_PROFILE") or "api"
            sql_query = f"UPDATE {table_path} SET is_deleted = true, updated_at = ?, updated_by = ? WHERE {request.key_column} = ?"
            params = [now, updated_by, request.key_value]
            try:
                db_connector.query(sql_query, warehouse_id=warehouse_id, params=params)
                return TableResponse(data=[{"is_deleted": True}], count=1, total=None)
            except Exception as e:
                raise DatabaseError(message=f"Failed to soft-delete orders record: {e}")

    # Otherwise perform hard delete
    try:
        sql_query = f"DELETE FROM {table_path} WHERE {request.key_column} = ?"
        params = [request.key_value]
        db_connector.query(sql_query, warehouse_id=warehouse_id, params=params)
        return TableResponse(data=[], count=1, total=None)
    except Exception as e:
        raise DatabaseError(message=f"Failed to delete orders record: {e}")
