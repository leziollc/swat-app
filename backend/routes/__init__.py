"""Routes package for the FastAPI application."""

from fastapi import APIRouter

# Import router factory from versioned packages
from .v1 import create_router

# Create the v1 router
v1_router = create_router()

# Create a router for the API
api_router = APIRouter()

# Include versioned routers - prefix must have /api for Databricks Apps token-based auth
api_router.include_router(v1_router, prefix="/api/v1")
