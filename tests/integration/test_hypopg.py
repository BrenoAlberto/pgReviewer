"""Contract tests: HypoPG session fixtures, validate recommendation logic.

These tests require a live PostgreSQL connection with the HypoPG extension
installed.  Run them with::

    pytest tests/integration/ -m integration

They are skipped automatically when ``SKIP_INTEGRATION_TESTS=1`` is set.

The tests cover:
- ``validate_candidate`` returns ``validated=True`` and
  ``improvement_pct > 0.90`` for a query with a known missing index.
- After ``validate_candidate`` exits, ``hypopg_list_indexes()`` returns
  0 rows (cleanup worked correctly).
- When HypoPG is not installed, ``validate_candidate`` raises
  ``ExtensionMissingError`` instead of a raw Postgres error.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any
from unittest.mock import AsyncMock

import asyncpg
import pytest

from pgreviewer.analysis.hypopg_validator import validate_candidate
from pgreviewer.analysis.index_suggester import IndexCandidate
from pgreviewer.exceptions import ExtensionMissingError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TABLE = "_pgr_test_hypopg"
_QUERY = f"SELECT * FROM {_TABLE} WHERE user_id = 42"


def _db_url() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/pgreviewer",
    )


def _candidate(table: str = _TABLE) -> IndexCandidate:
    return IndexCandidate(
        table=table,
        columns=["user_id"],
        index_type="btree",
        rationale="Btree index on user_id for equality filter",
    )


# ---------------------------------------------------------------------------
# Module-level fixture: create and populate the test table once
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def create_test_table() -> None:
    """Create a 50,000-row table used by all integration tests in this module.

    The table has no index on ``user_id`` so the planner will choose a
    sequential scan until a hypothetical index is created via HypoPG.
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
# Integration tests: live DB required
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_validate_candidate_returns_validated_true_with_high_improvement(
    create_test_table: None,
) -> None:
    """For a query with a known missing index, validate_candidate must return
    validated=True and improvement_pct > 0.90.

    The test table has 50,000 rows and no index on user_id, so the baseline
    plan is a sequential scan.  A hypothetical btree index on user_id makes
    an equality filter extremely selective (≈5 rows out of 50,000), causing
    the planner to switch to an index scan and dramatically reducing the
    estimated cost.
    """

    async def _run() -> None:
        conn = await asyncpg.connect(_db_url())
        try:
            result = await validate_candidate(_candidate(), _QUERY, conn)
        finally:
            await conn.close()

        assert result.validated is True, (
            "Expected validated=True but got "
            f"improvement_pct={result.improvement_pct:.4f}"
        )
        assert result.improvement_pct > 0.90, (
            f"Expected improvement_pct > 0.90, got {result.improvement_pct:.4f}"
        )

    asyncio.run(_run())


@pytest.mark.integration
def test_validate_candidate_cleans_up_hypothetical_index(
    create_test_table: None,
) -> None:
    """After validate_candidate exits, hypopg_list_indexes() must return 0 rows.

    HypoPG hypothetical indexes are session-scoped, so the check must use
    the same connection that was passed to validate_candidate.
    """

    async def _run() -> None:
        conn = await asyncpg.connect(_db_url())
        try:
            # Ensure no stale hypothetical indexes from a previous run
            await conn.execute("SELECT hypopg_reset()")

            await validate_candidate(_candidate(), _QUERY, conn)

            rows = await conn.fetch("SELECT * FROM hypopg_list_indexes()")
            assert len(rows) == 0, (
                f"Expected 0 hypothetical indexes after validate_candidate, "
                f"found {len(rows)}: {rows}"
            )
        finally:
            await conn.close()

    asyncio.run(_run())


@pytest.mark.integration
def test_validate_candidate_cleanup_on_explain_failure(
    create_test_table: None,
) -> None:
    """Hypothetical indexes must be dropped even when the second EXPLAIN fails.

    This test verifies the finally-block cleanup path in validate_candidate.
    """

    async def _run() -> None:
        conn = await asyncpg.connect(_db_url())
        try:
            await conn.execute("SELECT hypopg_reset()")

            # Monkeypatch: replace fetchval so the second EXPLAIN raises
            call_count = 0
            original_fetchval = conn.fetchval

            async def _patched_fetchval(query: str, *args: Any, **kwargs: Any) -> str:
                nonlocal call_count
                call_count += 1
                if call_count == 2:
                    raise RuntimeError("Simulated EXPLAIN failure after index creation")
                return await original_fetchval(query, *args, **kwargs)

            conn.fetchval = _patched_fetchval  # type: ignore[method-assign]

            with pytest.raises(RuntimeError, match="Simulated EXPLAIN failure"):
                await validate_candidate(_candidate(), _QUERY, conn)

            # Cleanup should have run despite the error
            rows = await conn.fetch("SELECT * FROM hypopg_list_indexes()")
            assert len(rows) == 0, (
                f"Expected 0 hypothetical indexes after failed validate_candidate, "
                f"found {len(rows)}"
            )
        finally:
            await conn.close()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Behaviour test: ExtensionMissingError (mocked connection — no live DB needed)
# ---------------------------------------------------------------------------


def test_validate_candidate_raises_extension_missing_error_when_hypopg_absent() -> None:
    """When HypoPG is not installed, validate_candidate must raise
    ExtensionMissingError with a clear message instead of a raw asyncpg error.

    This test uses a mocked connection so it does not require a live database.
    It is intentionally *not* marked @pytest.mark.integration so that it runs
    in every test environment and documents the expected contract.
    """
    import json

    conn = AsyncMock()
    conn.fetchval = AsyncMock(
        return_value=json.dumps(
            [
                {
                    "Plan": {
                        "Node Type": "Seq Scan",
                        "Total Cost": 1000.0,
                        "Startup Cost": 0.0,
                        "Plan Rows": 1000,
                        "Plan Width": 8,
                    }
                }
            ]
        )
    )
    conn.fetchrow = AsyncMock(
        side_effect=asyncpg.UndefinedFunctionError(
            "function hypopg_create_index(text) does not exist"
        )
    )

    with pytest.raises(ExtensionMissingError) as exc_info:
        asyncio.run(
            validate_candidate(
                _candidate(),
                _QUERY,
                conn,
            )
        )

    assert "hypopg" in str(exc_info.value)
    assert exc_info.value.extension == "hypopg"
    assert "CREATE EXTENSION hypopg" in str(exc_info.value)
