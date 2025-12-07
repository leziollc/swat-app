"""Routes package for the FastAPI application."""

from fastapi import APIRouter

from .v1 import create_router

v1_router = create_router()

api_router = APIRouter()

api_router.include_router(v1_router, prefix="/api/v1")
