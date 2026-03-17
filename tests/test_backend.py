from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from pgreviewer.config import Settings
from pgreviewer.core.backend import (
    HybridBackend,
    LocalBackend,
    MCPBackend,
    get_backend,
)
from pgreviewer.core.models import IndexRecommendation, SlowQuery, TableInfo
from pgreviewer.exceptions import MCPConnectionError


def _settings(backend: str) -> Settings:
    return Settings(
        DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres",
        BACKEND=backend,
    )


def test_get_backend_returns_local_backend() -> None:
    with patch("pgreviewer.core.backend.MCPClient.is_available") as mock_is_available:
        backend = get_backend(_settings("local"))
    assert isinstance(backend, LocalBackend)
    mock_is_available.assert_not_called()


def test_get_backend_returns_mcp_backend() -> None:
    with patch(
        "pgreviewer.core.backend.MCPClient.is_available",
        return_value=True,
    ):
        backend = get_backend(_settings("mcp"))
    assert isinstance(backend, MCPBackend)


def test_get_backend_returns_hybrid_backend() -> None:
    with patch(
        "pgreviewer.core.backend.MCPClient.is_available",
        return_value=True,
    ):
        backend = get_backend(_settings("hybrid"))
    assert isinstance(backend, HybridBackend)


def test_get_backend_rejects_unknown_backend() -> None:
    with pytest.raises(ValueError):
        get_backend(_settings("unknown"))


def test_get_backend_falls_back_to_local_when_mcp_unavailable() -> None:
    with (
        patch(
            "pgreviewer.core.backend.MCPClient.is_available",
            return_value=False,
        ),
        patch("pgreviewer.core.backend.logger.warning") as mock_warning,
    ):
        backend = get_backend(_settings("mcp"))

    assert isinstance(backend, LocalBackend)
    mock_warning.assert_called_once_with(
        "MCP Pro unavailable, falling back to local backend"
    )


def test_get_backend_falls_back_to_local_when_hybrid_mcp_unavailable() -> None:
    with (
        patch(
            "pgreviewer.core.backend.MCPClient.is_available",
            return_value=False,
        ),
        patch("pgreviewer.core.backend.logger.warning") as mock_warning,
    ):
        backend = get_backend(_settings("hybrid"))

    assert isinstance(backend, LocalBackend)
    mock_warning.assert_called_once_with(
        "MCP Pro unavailable, falling back to local backend"
    )


@pytest.mark.asyncio
async def test_hybrid_backend_routes_calls() -> None:
    local = AsyncMock(spec=LocalBackend)
    mcp = AsyncMock(spec=MCPBackend)
    hybrid = HybridBackend(local=local, mcp=mcp)

    local.get_explain_plan.return_value = {"Plan": {"Total Cost": 10}}
    mcp.recommend_indexes.return_value = [IndexRecommendation("orders", ["user_id"])]
    mcp.get_schema_info.return_value = TableInfo(row_estimate=100)
    mcp.get_slow_queries.return_value = [
        SlowQuery("SELECT 1", 1, 1.0, 1.0, 1),
    ]

    await hybrid.get_explain_plan(
        "SELECT 1",
        ["CREATE INDEX ON orders(user_id)"],
    )
    await hybrid.recommend_indexes(["SELECT 1"])
    await hybrid.get_schema_info("orders")
    await hybrid.get_slow_queries()

    local.get_explain_plan.assert_awaited_once_with(
        "SELECT 1",
        ["CREATE INDEX ON orders(user_id)"],
    )
    mcp.recommend_indexes.assert_awaited_once_with(["SELECT 1"])
    mcp.get_schema_info.assert_awaited_once_with("orders")
    mcp.get_slow_queries.assert_awaited_once_with(limit=20)


@pytest.mark.asyncio
async def test_mcp_backend_uses_mcp_wrappers() -> None:
    backend = MCPBackend("http://localhost:8000/mcp")
    conn = object()

    with (
        patch(
            "pgreviewer.core.backend.MCPClient.__aenter__",
            AsyncMock(return_value=conn),
        ),
        patch(
            "pgreviewer.core.backend.MCPClient.__aexit__",
            AsyncMock(return_value=None),
        ),
        patch(
            "pgreviewer.core.backend.mcp_get_explain_plan",
            AsyncMock(return_value={"Plan": {"Total Cost": 42}}),
        ) as mock_explain,
        patch(
            "pgreviewer.core.backend.mcp_recommend_indexes",
            AsyncMock(return_value=[]),
        ) as mock_recommend,
        patch(
            "pgreviewer.core.backend.mcp_get_schema_info",
            AsyncMock(return_value=TableInfo()),
        ) as mock_schema,
        patch(
            "pgreviewer.core.backend.mcp_get_slow_queries",
            AsyncMock(return_value=[]),
        ) as mock_slow,
    ):
        await backend.get_explain_plan("SELECT 1")
        await backend.recommend_indexes(["SELECT 1"])
        await backend.get_schema_info("orders")
        await backend.get_slow_queries(limit=5)

    mock_explain.assert_awaited_once_with("SELECT 1", conn, [])
    mock_recommend.assert_awaited_once_with(["SELECT 1"], conn)
    mock_schema.assert_awaited_once_with("orders", conn)
    mock_slow.assert_awaited_once_with(conn, limit=5)


@pytest.mark.asyncio
async def test_mcp_backend_falls_back_to_local_when_mcp_call_fails() -> None:
    local = AsyncMock(spec=LocalBackend)
    local.get_explain_plan.return_value = {"Plan": {"Total Cost": 10}}
    local.recommend_indexes.return_value = [IndexRecommendation("orders", ["user_id"])]
    local.get_schema_info.return_value = TableInfo(row_estimate=100)
    local.get_slow_queries.return_value = [
        SlowQuery("SELECT 1", 1, 1.0, 1.0, 1),
    ]
    backend = MCPBackend("http://localhost:8000/mcp", local=local)

    with (
        patch(
            "pgreviewer.core.backend.MCPClient.__aenter__",
            AsyncMock(side_effect=MCPConnectionError("down")),
        ),
        patch("pgreviewer.core.backend.logger.warning") as mock_warning,
    ):
        await backend.get_explain_plan("SELECT 1", ["CREATE INDEX ON orders(user_id)"])
        await backend.recommend_indexes(["SELECT 1"])
        await backend.get_schema_info("orders")
        await backend.get_slow_queries(limit=5)

    local.get_explain_plan.assert_awaited_once_with(
        "SELECT 1",
        ["CREATE INDEX ON orders(user_id)"],
    )
    local.recommend_indexes.assert_awaited_once_with(["SELECT 1"])
    local.get_schema_info.assert_awaited_once_with("orders")
    local.get_slow_queries.assert_awaited_once_with(limit=5)
    assert mock_warning.call_count == 4
