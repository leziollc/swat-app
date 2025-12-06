import logging
import os
import re
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ...models.lakebase import LakebaseResourcesDeleteResponse, LakebaseResourcesResponse
from ...services.db.connector import query

logger = logging.getLogger(__name__)
router = APIRouter(tags=["lakebase"])


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str) -> bool:
    return bool(IDENTIFIER_RE.match(name))


@router.post(
    "/resources/create-uc-resources",
    response_model=LakebaseResourcesResponse,
    summary="Create Unity Catalog schema and table",
)
async def create_uc_resources(
    schema: str = Query(
        ..., description="Schema name to create inside the configured catalog"
    ),
    table: str = Query(..., description="Table name to create inside the schema"),
    create_resources: bool = Query(
        False,
        description=(
            "Set to true to actually create the schema and table. "
            "If false, the endpoint will only validate and return what it would create."
        ),
    ),
    warehouse_id: Optional[str] = Query(
        None, description="Optional warehouse id to override environment setting"
    ),
):
    """Create a Unity Catalog schema and a simple table via the SQL warehouse.

    This endpoint executes SQL commands against the configured SQL warehouse.
    """

    # validate identifiers to avoid SQL injection
    if not _validate_identifier(schema):
        raise HTTPException(status_code=400, detail="Invalid schema name")
    if not _validate_identifier(table):
        raise HTTPException(status_code=400, detail="Invalid table name")

    # read config from environment (settings via .env)
    catalog = os.getenv("DATABRICKS_CATALOG_NAME") or os.getenv("LAKEBASE_CATALOG_NAME")
    if not catalog:
        raise HTTPException(status_code=500, detail="Catalog name not configured in environment")

    # warehouse id
    if not warehouse_id:
        warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID") or os.getenv("DATABRICKS_WAREHOUSE")
    if not warehouse_id:
        raise HTTPException(status_code=500, detail="SQL warehouse ID not configured in environment")

    schema_full = f"{catalog}.{schema}"
    table_full = f"{catalog}.{schema}.{table}"

    # SQL statements
    create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_full}"
    # simple table schema; callers can alter later as needed
    create_table_sql = (
        f"CREATE TABLE IF NOT EXISTS {table_full} (id BIGINT, payload STRING, created_at TIMESTAMP)"
    )

    if not create_resources:
        message = f"Would create schema {schema_full} and table {table_full} on warehouse {warehouse_id}"
        return LakebaseResourcesResponse(instance="", catalog=catalog, synced_table=table_full, message=message)

    # execute SQL via connector
    try:
        query(create_schema_sql, warehouse_id=warehouse_id)
        query(create_table_sql, warehouse_id=warehouse_id)
    except Exception as e:
        logger.error(f"Failed to create schema/table: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create schema/table: {str(e)}")

    message = f"Created schema {schema_full} and table {table_full} on warehouse {warehouse_id}"
    return LakebaseResourcesResponse(instance="", catalog=catalog, synced_table=table_full, message=message)


@router.delete(
    "/resources/delete-uc-resources",
    response_model=LakebaseResourcesDeleteResponse,
    summary="Delete Unity Catalog schema and table",
)
async def delete_uc_resources(
    schema: str = Query(..., description="Schema name to delete"),
    table: str = Query(..., description="Table name to delete"),
    confirm_deletion: bool = Query(False, description="Set to true to perform deletion"),
    warehouse_id: Optional[str] = Query(None, description="Optional warehouse id to override environment setting"),
):
    if not confirm_deletion:
        return LakebaseResourcesDeleteResponse(deleted_resources=[], failed_deletions=[], message="No resources deleted (confirm_deletion=False)")

    if not _validate_identifier(schema) or not _validate_identifier(table):
        raise HTTPException(status_code=400, detail="Invalid schema or table name")

    catalog = os.getenv("DATABRICKS_CATALOG_NAME") or os.getenv("LAKEBASE_CATALOG_NAME")
    if not catalog:
        raise HTTPException(status_code=500, detail="Catalog name not configured in environment")

    if not warehouse_id:
        warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID") or os.getenv("DATABRICKS_WAREHOUSE")
    if not warehouse_id:
        raise HTTPException(status_code=500, detail="SQL warehouse ID not configured in environment")

    schema_full = f"{catalog}.{schema}"
    table_full = f"{catalog}.{schema}.{table}"

    deleted = []
    failed = []

    # Drop table
    try:
        drop_table_sql = f"DROP TABLE IF EXISTS {table_full}"
        query(drop_table_sql, warehouse_id=warehouse_id)
        deleted.append(f"Table: {table_full}")
    except Exception as e:
        logger.error(f"Failed to drop table {table_full}: {e}")
        failed.append(f"Table: {table_full} - {str(e)}")

    # Drop schema (cascade)
    try:
        drop_schema_sql = f"DROP SCHEMA IF EXISTS {schema_full} CASCADE"
        query(drop_schema_sql, warehouse_id=warehouse_id)
        deleted.append(f"Schema: {schema_full}")
    except Exception as e:
        logger.error(f"Failed to drop schema {schema_full}: {e}")
        failed.append(f"Schema: {schema_full} - {str(e)}")

    if failed:
        message = f"Deletion completed with errors: {len(failed)} failures"
    else:
        message = f"Deleted {len(deleted)} resources successfully"

    return LakebaseResourcesDeleteResponse(deleted_resources=deleted, failed_deletions=failed, message=message)
