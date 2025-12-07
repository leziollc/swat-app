"""
Application settings using Pydantic BaseSettings.

This module defines the application settings that can be set
via environment variables.
"""


from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""

    # Databricks SQL warehouse ID
    databricks_warehouse_id: str | None = Field(
        default=None,
        description="The ID of the Databricks SQL warehouse to connect to",
    )

    # Use model_config instead of class Config
    model_config = {
        "env_file": ".env",
        "case_sensitive": False,
        "extra": "allow",
    }


# Create a singleton instance of the settings
settings = Settings()


def get_settings() -> Settings:
    """
    Get the application settings.

    This function is provided as a dependency for FastAPI endpoints.

    Returns:
        The application settings
    """
    return settings
