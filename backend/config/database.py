"""
Simplified database helpers for projects that no longer use Lakebase.

This module keeps the same public functions imported by the app, but it
removes runtime Lakebase/WorkspaceClient initialization. When Lakebase
is not enabled the functions are noop or will return sensible defaults.

Keep this module minimal so the application can run when Unity Catalog
is used via the SQL warehouse connector instead of Lakebase-managed Postgres.
"""

from typing import AsyncGenerator
import logging

logger = logging.getLogger(__name__)


async def init_engine() -> None:
    """No-op init engine for environments without Lakebase.

    Left in place for backwards compatibility with the application startup
    lifecycle which imports `init_engine`.
    """
    logger.info("Lakebase engine init skipped (using Unity Catalog SQL connector)")


async def start_token_refresh() -> None:
    logger.info("Lakebase token refresh skipped")


async def stop_token_refresh() -> None:
    logger.info("Lakebase token refresh stop skipped")


async def get_async_db() -> AsyncGenerator[None, None]:
    """Provide a placeholder dependency for routes that still depend on Async DB.

    Routes should be migrated to use the SQL connector. If a route still
    depends on `get_async_db`, this will raise an informative error at runtime.
    """
    raise RuntimeError("Lakebase is not configured in this deployment")


def check_database_exists() -> bool:
    """Indicate Lakebase database instance does not exist in modern deployments."""
    return False


async def database_health() -> bool:
    logger.info("Lakebase database health not applicable")
    return False
