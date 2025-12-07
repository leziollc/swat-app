"""
Main FastAPI application.

This module creates and configures the FastAPI application.
"""

import logging
import time
from contextlib import asynccontextmanager

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Request

from .errors.handlers import register_exception_handlers
from .routes import api_router

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown events."""
    logger.info("Application startup initiated")
    logger.info("Application startup complete")

    yield

    logger.info("Application shutdown initiated")
    from .services.db.connector import close_connections
    close_connections()
    logger.info("Database connections closed")
    logger.info("Application shutdown complete")


app = FastAPI(
    title="FastAPI & Databricks App",
    description="A FastAPI application for Databricks Apps runtime",
    version="1.0.0",
    lifespan=lifespan,
)

@app.middleware("http")
async def cache_request_body_and_track_time(request: Request, call_next):
    """Cache request body and track execution time so error handlers can access them."""
    import time

    start_time = time.perf_counter()
    request.state.start_time = start_time

    if request.method in ["POST", "PUT", "DELETE", "PATCH"]:
        body = await request.body()

        request._body = body

        async def receive():
            return {"type": "http.request", "body": body}

        request._receive = receive

    try:
        response = await call_next(request)
    finally:
        execution_time_ms = (time.perf_counter() - start_time) * 1000
        request.state.execution_time_ms = execution_time_ms

    return response

register_exception_handlers(app)

app.include_router(api_router)


@app.get("/")
async def root() -> dict[str, str]:
    return {
        "app": "Databricks FastAPI App (Developed by Dami Alebiosu)",
        "message": "Welcome to the Databricks FastAPI app",
        "docs": "/docs",
    }


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    logger.info(
        f"Request: {request.method} {request.url.path} - {process_time * 1000:.1f}ms"
    )
    return response


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
