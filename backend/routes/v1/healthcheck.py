"""Healthcheck endpoint for the V1 API."""

import os
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter

router = APIRouter()


@router.get("/healthcheck")
async def healthcheck() -> dict[str, Any]:
    """Return the API status and database connectivity."""
    status = "healthy"
    api_status = "up"
    db_status = "unknown"

    try:
        from ...services.db import connector as db_connector

        warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
        if not warehouse_id:
            db_status = "error: DATABRICKS_WAREHOUSE_ID not set"
            status = "degraded"
        else:
            db_connector.query("SELECT 1 AS health_check", warehouse_id=warehouse_id)
            db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"
        status = "degraded"

    return {
        "status": status,
        "components": {
            "api": api_status,
            "database": db_status,
        },
        "timestamp": datetime.now(UTC).isoformat(),
    }
