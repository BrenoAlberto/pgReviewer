from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import asyncpg

from pgreviewer.config import settings
from pgreviewer.exceptions import DBConnectionError

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    """
    Async factory that creates an asyncpg pool from settings.DATABASE_URL.

    Returns:
        asyncpg.Pool: The database connection pool.

    Raises:
        DBConnectionError: If the pool cannot be created or connection fails.
    """
    global _pool
    if _pool is None:
        try:
            # PostgresDsn from pydantic-settings needs to be converted to str
            dsn = str(settings.DATABASE_URL)
            _pool = await asyncpg.create_pool(dsn)
            if _pool is None:
                raise DBConnectionError(
                    "Failed to initialize database pool (pool is None)."
                )

            # Check for hypopg extension
            async with _pool.acquire() as conn:
                hypo_exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'hypopg')"
                )
                if not hypo_exists:
                    raise DBConnectionError(
                        "HypoPG extension not installed. "
                        "Please run 'CREATE EXTENSION hypopg;'."
                    )
        except asyncpg.InvalidPasswordError as e:
            raise DBConnectionError(
                f"Database connection failed: Invalid password. {e}"
            ) from e
        except asyncpg.InvalidCatalogNameError as e:
            raise DBConnectionError(
                f"Database connection failed: Database does not exist. {e}"
            ) from e
        except Exception as e:
            raise DBConnectionError(
                f"Could not connect to database at {settings.DATABASE_URL}: {e}"
            ) from e
    return _pool


async def close_pool() -> None:
    """Closes the database pool if it exists."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


@asynccontextmanager
async def read_session() -> AsyncGenerator[asyncpg.Connection, None]:
    """
    Async context manager that acquires a connection with read-only mode enabled.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Save current state if we wanted to be perfectly clean,
        # but the requirement says SET it to on.
        await conn.execute("SET default_transaction_read_only = on")
        try:
            yield conn
        finally:
            # Restore to off when returning to pool to avoid affecting other users
            await conn.execute("SET default_transaction_read_only = off")


@asynccontextmanager
async def write_session() -> AsyncGenerator[asyncpg.Connection, None]:
    """
    Async context manager that acquires a connection in a transaction
    that is ALWAYS rolled back on exit.

    Used exclusively for HypoPG operations to ensure no DDL or hypothetical
    indexes persist beyond the session.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            yield conn
        finally:
            await tx.rollback()
