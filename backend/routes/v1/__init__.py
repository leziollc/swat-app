"""V1 API routes."""

import logging

from fastapi import APIRouter

from .healthcheck import router as healthcheck_router
from .records import router as records_router

logger = logging.getLogger(__name__)


def create_router() -> APIRouter:
    """Create API router with all endpoints"""
    router = APIRouter()

    # Include all endpoints
    router.include_router(healthcheck_router)
    router.include_router(records_router, prefix="/records")

    logger.info("All API endpoints registered")

    return router

