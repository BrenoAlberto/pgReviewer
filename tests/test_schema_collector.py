from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from pgreviewer.analysis.schema_collector import clear_cache, collect_schema
from pgreviewer.core.models import ColumnInfo, IndexInfo, SchemaInfo, TableInfo


def _record(**kwargs):
    """Build a dict-like mock that supports subscript access."""
    rec = MagicMock()
    rec.__getitem__ = lambda self, key: kwargs[key]
    rec.get = lambda key, default=None: kwargs.get(key, default)
    return rec


@pytest.fixture(autouse=True)
def _clear_schema_cache():
    """Clear the module-level cache before each test."""
    clear_cache()
    yield
    clear_cache()


# --------------------------------------------------------------------------- #
# Table stats
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_collect_schema_returns_table_stats():
    conn = AsyncMock()
    call_count = 0

    async def _fetch(_query, _tables):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return [
                _record(
                    table_name="orders", row_estimate=50000, size_bytes=4096000
                ),
            ]
        return []

    conn.fetch = _fetch

    schema = await collect_schema(["orders"], conn)

    assert isinstance(schema, SchemaInfo)
    assert "orders" in schema.tables
    t = schema.tables["orders"]
    assert isinstance(t, TableInfo)
    assert t.row_estimate == 50000
    assert t.size_bytes == 4096000


# --------------------------------------------------------------------------- #
# Index definitions
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_collect_schema_returns_indexes():
    conn = AsyncMock()
    call_count = 0

    async def _fetch(_query, _tables):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return [
                _record(table_name="orders", row_estimate=1000, size_bytes=8192),
            ]
        if call_count == 2:
            return [
                _record(
                    index_name="idx_orders_user_id",
                    table_name="orders",
                    is_unique=False,
                    predicate=None,
                    index_type="btree",
                    columns=["user_id"],
                ),
                _record(
                    index_name="orders_pkey",
                    table_name="orders",
                    is_unique=True,
                    predicate=None,
                    index_type="btree",
                    columns=["id"],
                ),
            ]
        return []

    conn.fetch = _fetch

    schema = await collect_schema(["orders"], conn)

    indexes = schema.tables["orders"].indexes
    assert len(indexes) == 2

    idx = next(i for i in indexes if i.name == "idx_orders_user_id")
    assert isinstance(idx, IndexInfo)
    assert idx.columns == ["user_id"]
    assert idx.is_unique is False
    assert idx.is_partial is False
    assert idx.index_type == "btree"

    pk = next(i for i in indexes if i.name == "orders_pkey")
    assert pk.is_unique is True


# --------------------------------------------------------------------------- #
# Partial index detection
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_partial_index_flag():
    conn = AsyncMock()
    call_count = 0

    async def _fetch(_query, _tables):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return [
                _record(table_name="orders", row_estimate=100, size_bytes=1024),
            ]
        if call_count == 2:
            return [
                _record(
                    index_name="idx_orders_active",
                    table_name="orders",
                    is_unique=False,
                    predicate="(active = true)",
                    index_type="btree",
                    columns=["status"],
                ),
            ]
        return []

    conn.fetch = _fetch

    schema = await collect_schema(["orders"], conn)
    idx = schema.tables["orders"].indexes[0]
    assert idx.is_partial is True


# --------------------------------------------------------------------------- #
# Column statistics
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_collect_schema_returns_column_stats():
    conn = AsyncMock()
    call_count = 0

    async def _fetch(_query, _tables):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return [
                _record(table_name="users", row_estimate=200, size_bytes=2048),
            ]
        if call_count == 2:
            return []
        return [
            _record(
                table_name="users",
                column_name="email",
                column_type="text",
                null_fraction=0.01,
                distinct_count=-0.99,
            ),
        ]

    conn.fetch = _fetch

    schema = await collect_schema(["users"], conn)

    cols = schema.tables["users"].columns
    assert len(cols) == 1

    col = cols[0]
    assert isinstance(col, ColumnInfo)
    assert col.name == "email"
    assert col.type == "text"
    assert col.null_fraction == pytest.approx(0.01)
    assert col.distinct_count == pytest.approx(-0.99)


# --------------------------------------------------------------------------- #
# Missing tables get empty TableInfo
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_missing_table_gets_empty_entry():
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=[])

    schema = await collect_schema(["nonexistent"], conn)

    assert "nonexistent" in schema.tables
    t = schema.tables["nonexistent"]
    assert t.row_estimate == 0
    assert t.size_bytes == 0
    assert t.indexes == []
    assert t.columns == []


# --------------------------------------------------------------------------- #
# Caching
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_collect_schema_caches_results():
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=[])

    schema1 = await collect_schema(["orders"], conn)
    schema2 = await collect_schema(["orders"], conn)

    assert schema1 is schema2
    # fetch is called 3 times per collect_schema call (table, index, column).
    # With caching, only the first call should trigger queries.
    assert conn.fetch.await_count == 3


@pytest.mark.asyncio
async def test_different_tables_not_cached():
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=[])

    await collect_schema(["orders"], conn)
    await collect_schema(["users"], conn)

    # 3 queries per distinct table set
    assert conn.fetch.await_count == 6


@pytest.mark.asyncio
async def test_clear_cache_resets():
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=[])

    await collect_schema(["orders"], conn)
    clear_cache()
    await collect_schema(["orders"], conn)

    assert conn.fetch.await_count == 6
