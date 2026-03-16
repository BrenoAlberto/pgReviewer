"""Tests for pgreviewer.analysis.hypopg_validator."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from pgreviewer.analysis.hypopg_validator import (
    ValidationResult,
    _build_create_index_sql,
    _extract_root_cost,
    validate_candidate,
)
from pgreviewer.analysis.index_suggester import IndexCandidate

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_plan(total_cost: float) -> dict[str, Any]:
    """Build a minimal EXPLAIN JSON dict with the given root total cost."""
    return {
        "Plan": {
            "Node Type": "Seq Scan",
            "Total Cost": total_cost,
            "Startup Cost": 0.0,
            "Plan Rows": 1000,
            "Plan Width": 8,
        }
    }


def _make_conn(
    cost_before: float, cost_after: float, indexrelid: int = 42
) -> AsyncMock:
    """Build a mock asyncpg connection that simulates the HypoPG round-trip."""
    conn = AsyncMock()

    # hypopg_create_index returns a row with indexrelid
    hypo_row = MagicMock()
    hypo_row.__getitem__ = lambda self, key: indexrelid if key == "indexrelid" else None
    conn.fetchrow = AsyncMock(return_value=hypo_row)

    # hypopg_drop_index (execute)
    conn.execute = AsyncMock(return_value=None)

    # run_explain calls conn.fetchval for EXPLAIN queries
    import json

    call_count = 0

    async def _fetchval(query, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        cost = cost_before if call_count == 1 else cost_after
        return json.dumps([_make_plan(cost)])

    conn.fetchval = _fetchval
    return conn


# ---------------------------------------------------------------------------
# Unit: _build_create_index_sql
# ---------------------------------------------------------------------------


def test_build_create_index_sql_simple():
    candidate = IndexCandidate(
        table="orders",
        columns=["user_id"],
        index_type="btree",
        rationale="test",
    )
    sql = _build_create_index_sql(candidate)
    assert sql == "CREATE INDEX ON orders USING btree (user_id)"


def test_build_create_index_sql_composite():
    candidate = IndexCandidate(
        table="orders",
        columns=["user_id", "status"],
        index_type="btree",
        rationale="test",
    )
    sql = _build_create_index_sql(candidate)
    assert sql == "CREATE INDEX ON orders USING btree (user_id, status)"


def test_build_create_index_sql_partial():
    candidate = IndexCandidate(
        table="users",
        columns=["deleted_at"],
        index_type="btree",
        partial_predicate="deleted_at IS NOT NULL",
        rationale="test",
    )
    sql = _build_create_index_sql(candidate)
    assert sql == (
        "CREATE INDEX ON users USING btree (deleted_at) WHERE deleted_at IS NOT NULL"
    )


# ---------------------------------------------------------------------------
# Unit: _extract_root_cost
# ---------------------------------------------------------------------------


def test_extract_root_cost():
    plan = _make_plan(4500.0)
    assert _extract_root_cost(plan) == pytest.approx(4500.0)


# ---------------------------------------------------------------------------
# Unit: ValidationResult model
# ---------------------------------------------------------------------------


def test_validation_result_fields():
    result = ValidationResult(
        cost_before=4500.0,
        cost_after=8.0,
        improvement_pct=0.9982,
        new_plan=_make_plan(8.0),
        validated=True,
    )
    assert result.cost_before == pytest.approx(4500.0)
    assert result.cost_after == pytest.approx(8.0)
    assert result.improvement_pct == pytest.approx(0.9982)
    assert result.validated is True


# ---------------------------------------------------------------------------
# Integration: validate_candidate (fully mocked connection)
# ---------------------------------------------------------------------------


def test_validate_candidate_validated_when_large_improvement():
    """Cost dropping from ~4500 to ~8 (>99%) must yield validated=True."""
    candidate = IndexCandidate(
        table="orders",
        columns=["user_id"],
        index_type="btree",
        rationale="test",
    )
    conn = _make_conn(cost_before=4500.0, cost_after=8.0)

    result = asyncio.run(
        validate_candidate(
            candidate,
            "SELECT * FROM orders WHERE user_id = $1",
            conn,
        )
    )

    assert result.cost_before == pytest.approx(4500.0)
    assert result.cost_after == pytest.approx(8.0)
    assert result.improvement_pct == pytest.approx((4500.0 - 8.0) / 4500.0)
    assert result.validated is True


def test_validate_candidate_not_validated_when_small_improvement():
    """Cost dropping only 10% must yield validated=False (threshold is 30%)."""
    candidate = IndexCandidate(
        table="orders",
        columns=["user_id"],
        index_type="btree",
        rationale="test",
    )
    conn = _make_conn(cost_before=1000.0, cost_after=900.0)

    result = asyncio.run(
        validate_candidate(
            candidate,
            "SELECT * FROM orders WHERE user_id = $1",
            conn,
        )
    )

    assert result.improvement_pct == pytest.approx(0.10)
    assert result.validated is False


def test_validate_candidate_drops_index_on_success():
    """hypopg_drop_index must be called even when EXPLAIN succeeds."""
    candidate = IndexCandidate(
        table="orders",
        columns=["user_id"],
        index_type="btree",
        rationale="test",
    )
    conn = _make_conn(cost_before=1000.0, cost_after=10.0, indexrelid=99)

    asyncio.run(
        validate_candidate(
            candidate,
            "SELECT * FROM orders WHERE user_id = $1",
            conn,
        )
    )

    conn.execute.assert_awaited_once_with("SELECT hypopg_drop_index($1)", 99)


def test_validate_candidate_drops_index_on_explain_failure():
    """hypopg_drop_index must still be called when the second EXPLAIN raises."""
    import json

    candidate = IndexCandidate(
        table="orders",
        columns=["user_id"],
        index_type="btree",
        rationale="test",
    )

    conn = AsyncMock()
    hypo_row = MagicMock()
    hypo_row.__getitem__ = lambda self, key: 77 if key == "indexrelid" else None
    conn.fetchrow = AsyncMock(return_value=hypo_row)
    conn.execute = AsyncMock(return_value=None)

    call_count = 0

    async def _fetchval(query, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return json.dumps([_make_plan(1000.0)])
        # Raise RuntimeError directly; run_explain only catches asyncpg and JSON
        # errors, so this propagates unwrapped.
        raise RuntimeError("Simulated EXPLAIN failure")

    conn.fetchval = _fetchval

    with pytest.raises(RuntimeError, match="Simulated EXPLAIN failure"):
        asyncio.run(
            validate_candidate(
                candidate,
                "SELECT * FROM orders WHERE user_id = $1",
                conn,
            )
        )

    conn.execute.assert_awaited_once_with("SELECT hypopg_drop_index($1)", 77)


def test_validate_candidate_creates_correct_index_sql():
    """hypopg_create_index must be called with the correct CREATE INDEX statement."""
    candidate = IndexCandidate(
        table="users",
        columns=["deleted_at"],
        index_type="btree",
        partial_predicate="deleted_at IS NOT NULL",
        rationale="test",
    )
    conn = _make_conn(cost_before=500.0, cost_after=5.0)

    asyncio.run(
        validate_candidate(
            candidate,
            "SELECT * FROM users WHERE deleted_at IS NOT NULL",
            conn,
        )
    )

    conn.fetchrow.assert_awaited_once_with(
        "SELECT * FROM hypopg_create_index($1)",
        "CREATE INDEX ON users USING btree (deleted_at) WHERE deleted_at IS NOT NULL",
    )


def test_validate_candidate_no_improvement_when_cost_before_zero():
    """When cost_before is 0 (degenerate), improvement_pct must be 0.0."""
    candidate = IndexCandidate(
        table="orders",
        columns=["user_id"],
        index_type="btree",
        rationale="test",
    )
    conn = _make_conn(cost_before=0.0, cost_after=0.0)

    result = asyncio.run(validate_candidate(candidate, "SELECT 1", conn))

    assert result.improvement_pct == pytest.approx(0.0)
    assert result.validated is False


def test_validate_candidate_new_plan_matches_after_plan():
    """new_plan must reflect the plan returned after the hypothetical index."""
    candidate = IndexCandidate(
        table="orders",
        columns=["user_id"],
        index_type="btree",
        rationale="test",
    )
    conn = _make_conn(cost_before=4500.0, cost_after=8.0)

    result = asyncio.run(
        validate_candidate(
            candidate,
            "SELECT * FROM orders WHERE user_id = $1",
            conn,
        )
    )

    assert result.new_plan["Plan"]["Total Cost"] == pytest.approx(8.0)


def test_validate_candidate_improvement_thresholds():
    """Test specific boundaries: 35% (OK), 20% (Low), 99% (OK)."""
    candidate = IndexCandidate(
        table="orders",
        columns=["user_id"],
        index_type="btree",
        rationale="test",
    )

    # 35% Improvement
    conn_35 = _make_conn(cost_before=100.0, cost_after=65.0)
    res_35 = asyncio.run(validate_candidate(candidate, "SELECT 1", conn_35))
    assert res_35.improvement_pct == pytest.approx(0.35)
    assert res_35.validated is True

    # 20% Improvement
    conn_20 = _make_conn(cost_before=100.0, cost_after=80.0)
    res_20 = asyncio.run(validate_candidate(candidate, "SELECT 1", conn_20))
    assert res_20.improvement_pct == pytest.approx(0.20)
    assert res_20.validated is False
    assert (
        res_20.rationale
        == "Index would help slightly but may not justify the write overhead"
    )

    # 99% Improvement
    conn_99 = _make_conn(cost_before=100.0, cost_after=1.0)
    res_99 = asyncio.run(validate_candidate(candidate, "SELECT 1", conn_99))
    assert res_99.improvement_pct == pytest.approx(0.99)
    assert res_99.validated is True


def test_validate_candidate_no_improvement_message():
    """Cost dropping only 2% (<5%) must yield rationale about no
    significant improvement.
    """
    candidate = IndexCandidate(
        table="orders",
        columns=["user_id"],
        index_type="btree",
        rationale="test",
    )
    conn = _make_conn(cost_before=1000.0, cost_after=980.0)

    result = asyncio.run(
        validate_candidate(
            candidate,
            "SELECT * FROM orders WHERE user_id = $1",
            conn,
        )
    )

    assert result.validated is False
    assert result.rationale == "No significant improvement detected"


def test_validate_candidate_logs_to_debug_store():
    """validate_candidate should save validation details to the debug store."""
    candidate = IndexCandidate(
        table="orders",
        columns=["user_id"],
        index_type="btree",
        rationale="test",
    )
    conn = _make_conn(cost_before=1000.0, cost_after=100.0)
    debug_store = MagicMock()

    asyncio.run(
        validate_candidate(
            candidate,
            "SELECT * FROM orders WHERE user_id = $1",
            conn,
            run_id="test-run",
            debug_store=debug_store,
        )
    )

    debug_store.save.assert_called_once()
    args = debug_store.save.call_args[0]
    assert args[0] == "test-run"
    assert args[1] == "HYPOPG_VALIDATION"
    assert args[2]["improvement_pct"] == pytest.approx(0.9)
    assert args[2]["validated"] is True
