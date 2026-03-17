"""Integration tests for MCP client, wrappers, and MCPBackend.

These tests require a **running Postgres MCP Pro instance**.  They are
expensive and must be explicitly opted into — they never run as part of
the standard unit test suite.

Running the MCP tests
---------------------
Start a local Postgres MCP Pro instance (see README for full instructions),
then run::

    pytest -m mcp

To skip them in a CI pipeline that doesn't have MCP Pro available::

    pytest -m "not mcp"
    # or equivalently:
    SKIP_MCP_TESTS=1 pytest

Environment variables used
--------------------------
MCP_SERVER_URL   URL of the Postgres MCP Pro streamable-HTTP endpoint.
                 Defaults to ``http://localhost:8000/mcp``.
DATABASE_URL     PostgreSQL connection string used to seed the test table
                 and by the LocalBackend fallback assertion.
                 Defaults to ``postgresql://postgres:postgres@localhost:5432/pgreviewer``.

Test coverage
-------------
- MCPClient connection lifecycle: connect, call a tool, disconnect.
- Each MCP wrapper: ``mcp_get_explain_plan``, ``mcp_recommend_indexes``,
  ``mcp_get_schema_info``, ``mcp_get_slow_queries``.
- MCPBackend end-to-end: ``recommend_indexes`` against a seeded table via
  MCP Pro; asserts results are valid ``IndexRecommendation`` objects.
- Fallback: ``get_backend(BACKEND=mcp)`` with an unreachable MCP URL
  returns a ``LocalBackend`` and emits a WARNING log.
"""

from __future__ import annotations

import asyncio
import logging
import os
from unittest.mock import patch

import asyncpg
import pytest

from pgreviewer.core.backend import LocalBackend, MCPBackend, get_backend
from pgreviewer.core.models import IndexRecommendation, SlowQuery, TableInfo
from pgreviewer.exceptions import MCPConnectionError
from pgreviewer.mcp.client import MCPClient
from pgreviewer.mcp.wrappers import (
    clear_mcp_schema_cache,
    mcp_get_explain_plan,
    mcp_get_schema_info,
    mcp_get_slow_queries,
    mcp_recommend_indexes,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TABLE = "_pgr_mcp_integration_test"
_QUERY = f"SELECT * FROM {_TABLE} WHERE user_id = 42"
_BAD_MCP_URL = "http://127.0.0.1:19999/mcp"


def _mcp_url() -> str:
    return os.environ.get("MCP_SERVER_URL", "http://localhost:8000/mcp")


def _db_url() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/pgreviewer",
    )


# ---------------------------------------------------------------------------
# Module-level fixture: seeded test table
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def seeded_test_table() -> None:
    """Create a 50 000-row test table used by wrapper and backend tests.

    The table has no index on ``user_id`` so the planner chooses a
    sequential scan until a hypothetical (or real) index is provided.
    Skips the entire module if the database is unreachable.
    """

    async def _setup() -> None:
        conn = await asyncpg.connect(_db_url())
        try:
            await conn.execute(f"""
                DROP TABLE IF EXISTS {_TABLE};
                CREATE TABLE {_TABLE} (
                    id      SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    status  TEXT    NOT NULL
                );
                INSERT INTO {_TABLE} (user_id, status)
                SELECT (random() * 9999)::int + 1,
                       CASE WHEN random() > 0.5 THEN 'active' ELSE 'inactive' END
                FROM generate_series(1, 50000);
                ANALYZE {_TABLE};
            """)
        finally:
            await conn.close()

    async def _teardown() -> None:
        conn = await asyncpg.connect(_db_url())
        try:
            await conn.execute(f"DROP TABLE IF EXISTS {_TABLE};")
        finally:
            await conn.close()

    try:
        asyncio.run(_setup())
    except Exception as exc:
        pytest.skip(f"Cannot connect to test database: {exc}")

    yield

    asyncio.run(_teardown())


# ---------------------------------------------------------------------------
# MCPClient: connection lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.mcp
def test_mcp_client_connect_establishes_session() -> None:
    """MCPClient.connect() sets ``_session``; disconnect() clears it."""

    async def _run() -> None:
        client = MCPClient(_mcp_url())
        await client.connect()
        assert client._session is not None, "Session should be set after connect()"
        await client.disconnect()
        assert client._session is None, "Session should be cleared after disconnect()"

    asyncio.run(_run())


@pytest.mark.mcp
def test_mcp_client_context_manager_lifecycle() -> None:
    """Context manager connects on enter and disconnects on exit."""

    async def _run() -> None:
        async with MCPClient(_mcp_url()) as client:
            assert client._session is not None
        assert client._session is None

    asyncio.run(_run())


@pytest.mark.mcp
def test_mcp_client_can_call_tool_after_connect() -> None:
    """After connect(), calling a tool via the session succeeds."""

    async def _run() -> None:
        async with MCPClient(_mcp_url()) as client:
            response = await client._session.call_tool(
                "explain_query", {"sql": "SELECT 1", "analyze": False}
            )
            assert not getattr(response, "isError", False), (
                f"MCP tool call returned an error: {response}"
            )

    asyncio.run(_run())


@pytest.mark.mcp
def test_mcp_client_bad_url_raises_mcp_connection_error() -> None:
    """Connecting to a non-existent MCP server raises MCPConnectionError."""

    async def _run() -> None:
        client = MCPClient(_BAD_MCP_URL)
        with pytest.raises(MCPConnectionError):
            await client.connect()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Wrapper: mcp_get_explain_plan
# ---------------------------------------------------------------------------


@pytest.mark.mcp
def test_mcp_get_explain_plan_returns_plan_dict() -> None:
    """``mcp_get_explain_plan`` returns a dict with a top-level ``Plan`` key."""

    async def _run() -> dict:
        async with MCPClient(_mcp_url()) as client:
            return await mcp_get_explain_plan("SELECT 1", client)

    result = asyncio.run(_run())

    assert isinstance(result, dict), f"Expected dict, got {type(result)}"
    assert "Plan" in result, f"'Plan' key missing from explain response: {result}"


@pytest.mark.mcp
def test_mcp_get_explain_plan_accepts_hypothetical_indexes(
    seeded_test_table: None,
) -> None:
    """``mcp_get_explain_plan`` with ``hypothetical_indexes`` returns a plan."""

    hypo = f"CREATE INDEX ON {_TABLE} (user_id)"

    async def _run() -> dict:
        async with MCPClient(_mcp_url()) as client:
            return await mcp_get_explain_plan(
                _QUERY, client, hypothetical_indexes=[hypo]
            )

    result = asyncio.run(_run())

    assert isinstance(result, dict)
    assert "Plan" in result


# ---------------------------------------------------------------------------
# Wrapper: mcp_recommend_indexes
# ---------------------------------------------------------------------------


@pytest.mark.mcp
def test_mcp_recommend_indexes_returns_index_recommendations(
    seeded_test_table: None,
) -> None:
    """``mcp_recommend_indexes`` returns a list of ``IndexRecommendation``."""

    async def _run() -> list[IndexRecommendation]:
        async with MCPClient(_mcp_url()) as client:
            return await mcp_recommend_indexes([_QUERY], client)

    results = asyncio.run(_run())

    assert isinstance(results, list)
    for rec in results:
        assert isinstance(rec, IndexRecommendation), (
            f"Expected IndexRecommendation, got {type(rec)}"
        )
        assert rec.table, "IndexRecommendation.table must be non-empty"
        assert rec.columns, "IndexRecommendation.columns must be non-empty"
        assert isinstance(rec.create_statement, str)


@pytest.mark.mcp
def test_mcp_recommend_indexes_empty_input_returns_empty_list() -> None:
    """``mcp_recommend_indexes`` with an empty query list returns ``[]``."""

    async def _run() -> list:
        async with MCPClient(_mcp_url()) as client:
            return await mcp_recommend_indexes([], client)

    assert asyncio.run(_run()) == []


# ---------------------------------------------------------------------------
# Wrapper: mcp_get_schema_info
# ---------------------------------------------------------------------------


@pytest.mark.mcp
def test_mcp_get_schema_info_returns_table_info(
    seeded_test_table: None,
) -> None:
    """``mcp_get_schema_info`` returns a ``TableInfo`` for a known table."""

    async def _run() -> TableInfo:
        clear_mcp_schema_cache()
        async with MCPClient(_mcp_url()) as client:
            return await mcp_get_schema_info(_TABLE, client)

    info = asyncio.run(_run())

    assert isinstance(info, TableInfo), f"Expected TableInfo, got {type(info)}"


# ---------------------------------------------------------------------------
# Wrapper: mcp_get_slow_queries
# ---------------------------------------------------------------------------


@pytest.mark.mcp
def test_mcp_get_slow_queries_returns_slow_query_list() -> None:
    """``mcp_get_slow_queries`` returns a list of ``SlowQuery`` objects."""

    async def _run() -> list[SlowQuery]:
        async with MCPClient(_mcp_url()) as client:
            return await mcp_get_slow_queries(client, limit=5)

    results = asyncio.run(_run())

    assert isinstance(results, list)
    for item in results:
        assert isinstance(item, SlowQuery), f"Expected SlowQuery, got {type(item)}"
        assert isinstance(item.query_text, str)
        assert isinstance(item.calls, int)
        assert isinstance(item.mean_exec_time_ms, float)
        assert isinstance(item.total_exec_time_ms, float)


# ---------------------------------------------------------------------------
# MCPBackend: end-to-end
# ---------------------------------------------------------------------------


@pytest.mark.mcp
def test_mcp_backend_recommend_indexes_end_to_end(
    seeded_test_table: None,
) -> None:
    """MCPBackend.recommend_indexes runs against the seeded DB via MCP Pro
    and returns valid ``IndexRecommendation`` objects tagged ``source='mcp_pro'``.
    """

    async def _run() -> list[IndexRecommendation]:
        backend = MCPBackend(server_url=_mcp_url())
        return await backend.recommend_indexes([_QUERY])

    results = asyncio.run(_run())

    assert isinstance(results, list)
    for rec in results:
        assert isinstance(rec, IndexRecommendation), (
            f"Expected IndexRecommendation, got {type(rec)}"
        )
        assert rec.table, "table must be non-empty"
        assert rec.columns, "columns must be non-empty"
        assert isinstance(rec.create_statement, str)
        assert rec.source == "mcp_pro", f"Expected source='mcp_pro', got '{rec.source}'"


# ---------------------------------------------------------------------------
# Fallback: bad MCP URL → LocalBackend + warning log
# ---------------------------------------------------------------------------


@pytest.mark.mcp
def test_get_backend_falls_back_to_local_when_mcp_url_is_bad(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``get_backend(BACKEND=mcp)`` with an unreachable MCP URL returns
    a ``LocalBackend`` and emits a WARNING-level log message.

    ``MCPClient.is_available`` is patched to return ``False`` immediately so
    the test is fast and deterministic — the actual connection-refusal path
    is covered by ``test_mcp_client_bad_url_raises_mcp_connection_error``.
    """
    from pgreviewer.config import Settings

    bad_settings = Settings(
        DATABASE_URL=_db_url(),  # type: ignore[arg-type]
        BACKEND="mcp",
        MCP_SERVER_URL=_BAD_MCP_URL,
    )

    with (
        patch(
            "pgreviewer.core.backend.MCPClient.is_available",
            return_value=False,
        ),
        caplog.at_level(logging.WARNING, logger="pgreviewer.core.backend"),
    ):
        backend = get_backend(bad_settings)

    assert isinstance(backend, LocalBackend), (
        f"Expected LocalBackend fallback, got {type(backend)}"
    )
    warning_messages = [
        r.message for r in caplog.records if r.levelno >= logging.WARNING
    ]
    assert any(
        "unavailable" in msg.lower() or "fallback" in msg.lower()
        for msg in warning_messages
    ), f"Expected a fallback warning in logs, got: {warning_messages}"
