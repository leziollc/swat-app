"""
Exception handlers for the application.

This module defines handlers for exceptions that can be registered
with FastAPI to provide consistent error responses.
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError as PydanticValidationError

from ..services.logger import db_logger
from .exceptions import BaseAppException


def register_exception_handlers(app: FastAPI) -> None:
    """
    Register exception handlers with the FastAPI application.

    Args:
        app: The FastAPI application
    """

    @app.exception_handler(BaseAppException)
    async def handle_base_app_exception(
        request: Request, exc: BaseAppException
    ) -> JSONResponse:
        """Handle BaseAppException and its subclasses."""
        import time
        start_time = getattr(request.state, "start_time", None)
        execution_time_ms = (time.perf_counter() - start_time) * 1000 if start_time else None

        db_logger.log_error(
            exc,
            request=request,
            level="ERROR",
            additional_context={
                "status_code": exc.status_code,
                "execution_time_ms": execution_time_ms,
                **exc.details,
            },
        )

        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": True,
                "message": exc.message,
                "details": exc.details,
            },
        )

    @app.exception_handler(PydanticValidationError)
    async def handle_validation_error(
        request: Request, exc: PydanticValidationError
    ) -> JSONResponse:
        """Handle Pydantic validation errors."""
        from typing import Any
        errors = []
        for error in exc.errors():
            error_dict: dict[str, Any] = {
                "type": error["type"],
                "loc": list(error["loc"]),
                "msg": error["msg"],
            }
            if "input" in error:
                error_dict["input"] = str(error["input"])
            if "ctx" in error:
                error_dict["ctx"] = {k: str(v) for k, v in error["ctx"].items()}
            errors.append(error_dict)

        import time
        start_time = getattr(request.state, "start_time", None)
        execution_time_ms = (time.perf_counter() - start_time) * 1000 if start_time else None

        db_logger.log_error(
            exc,
            request=request,
            level="WARNING",
            additional_context={
                "status_code": 400,
                "execution_time_ms": execution_time_ms,
            },
        )

        return JSONResponse(
            status_code=400,
            content={
                "error": True,
                "message": "Validation error",
                "details": {"errors": errors},
            },
        )

    @app.exception_handler(Exception)
    async def handle_unhandled_exception(
        request: Request, exc: Exception
    ) -> JSONResponse:
        """Handle all unhandled exceptions."""
        import time
        start_time = getattr(request.state, "start_time", None)
        execution_time_ms = (time.perf_counter() - start_time) * 1000 if start_time else None

        db_logger.log_error(
            exc,
            request=request,
            level="ERROR",
            additional_context={
                "status_code": 500,
                "execution_time_ms": execution_time_ms,
            },
        )

        return JSONResponse(
            status_code=500,
            content={
                "error": True,
                "message": "Internal server error",
                "details": {"type": str(type(exc).__name__), "info": str(exc)},
            },
        )
