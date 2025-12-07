"""
Custom exceptions for the application.

This module defines custom exceptions that can be raised by the application
and handled by the global exception handlers.
"""

from typing import Any


class BaseAppException(Exception):
    """Base exception for all application exceptions."""

    def __init__(
        self,
        message: str,
        status_code: int = 500,
        details: dict[str, Any] | None = None,
    ):
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)


class DatabaseError(BaseAppException):
    """Exception raised when a database operation fails."""

    def __init__(
        self,
        message: str = "Database operation failed",
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message=message, status_code=500, details=details)


class ConfigurationError(BaseAppException):
    """Exception raised when a configuration value is missing or invalid."""

    def __init__(
        self,
        message: str = "Missing or invalid configuration",
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message=message, status_code=500, details=details)
