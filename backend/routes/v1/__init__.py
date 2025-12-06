"""V1 API routes."""

import logging
from fastapi import APIRouter

from .healthcheck import router as healthcheck_router
from .lakebase import router as lakebase_router

logger = logging.getLogger(__name__)


def create_router(database_exists: bool = False) -> APIRouter:
    """Create API router with conditional endpoint registration"""
    router = APIRouter()
    
    # Always include these endpoints
    router.include_router(healthcheck_router)
    router.include_router(lakebase_router)
    
    # Include database-related endpoints (orders, tables)
    try:
        from .orders import router as orders_router
        from .tables import router as tables_router

        router.include_router(tables_router, prefix="/tables")
        router.include_router(orders_router, prefix="/orders")
        logger.info("Database-related endpoints (orders, tables) registered")
    except Exception as e:
        logger.error(f"Failed to register orders/tables endpoints: {e}")
    
    return router
