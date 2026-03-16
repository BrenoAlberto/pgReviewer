"""Tests for multi-recommendation handling (ranking, diminishing returns,
redundancy detection, and combined HypoPG validation)."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from pgreviewer.analysis.hypopg_validator import (
    CombinedValidationResult,
    validate_candidates_combined,
)
from pgreviewer.analysis.index_suggester import IndexCandidate
from pgreviewer.cli.commands.check import _detect_redundant_recommendations
from pgreviewer.core.models import IndexRecommendation

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_plan(total_cost: float) -> dict[str, Any]:
    return {
        "Plan": {
            "Node Type": "Seq Scan",
            "Total Cost": total_cost,
            "Startup Cost": 0.0,
            "Plan Rows": 1000,
            "Plan Width": 8,
        }
    }


def _make_conn_multi(
    cost_before: float, cost_after: float, indexrelid_start: int = 10
) -> AsyncMock:
    """Mock connection for combined validation tests.

    Returns cost_before on the first fetchval call and cost_after on the second.
    fetchrow always returns a new indexrelid (incrementing from indexrelid_start).
    """
    conn = AsyncMock()

    call_count_fetchrow = 0

    async def _fetchrow(query, *args, **kwargs):
        nonlocal call_count_fetchrow
        call_count_fetchrow += 1
        row = MagicMock()
        row.__getitem__ = lambda self, key: (
            indexrelid_start + call_count_fetchrow - 1 if key == "indexrelid" else None
        )
        return row

    conn.fetchrow = _fetchrow
    conn.execute = AsyncMock(return_value=None)

    call_count_fetchval = 0

    async def _fetchval(query, *args, **kwargs):
        nonlocal call_count_fetchval
        call_count_fetchval += 1
        cost = cost_before if call_count_fetchval == 1 else cost_after
        return json.dumps([_make_plan(cost)])

    conn.fetchval = _fetchval
    return conn


def _make_rec(
    table: str,
    columns: list[str],
    improvement_pct: float,
    validated: bool = True,
    notes: list[str] | None = None,
) -> IndexRecommendation:
    return IndexRecommendation(
        table=table,
        columns=columns,
        index_type="btree",
        improvement_pct=improvement_pct,
        validated=validated,
        notes=notes if notes is not None else [],
    )


# ---------------------------------------------------------------------------
# validate_candidates_combined
# ---------------------------------------------------------------------------


def test_combined_validation_no_candidates():
    """With no candidates the combined cost equals the baseline."""
    conn = _make_conn_multi(cost_before=1000.0, cost_after=1000.0)
    result = asyncio.run(validate_candidates_combined([], "SELECT 1", conn))
    assert result.cost_before == pytest.approx(1000.0)
    assert result.cost_after == pytest.approx(1000.0)
    assert result.improvement_pct == pytest.approx(0.0)


def test_combined_validation_single_candidate():
    """Single candidate combined validation behaves like individual validation."""
    cand = IndexCandidate(
        table="orders",
        columns=["user_id"],
        index_type="btree",
        rationale="test",
    )
    conn = _make_conn_multi(cost_before=4000.0, cost_after=40.0)
    result = asyncio.run(
        validate_candidates_combined(
            [cand], "SELECT * FROM orders WHERE user_id = $1", conn
        )
    )
    assert result.cost_before == pytest.approx(4000.0)
    assert result.cost_after == pytest.approx(40.0)
    expected_pct = (4000.0 - 40.0) / 4000.0
    assert result.improvement_pct == pytest.approx(expected_pct)


def test_combined_validation_multiple_candidates():
    """Multiple candidates are created simultaneously; improvement reflects all."""
    candidates = [
        IndexCandidate(
            table="orders", columns=["user_id"], index_type="btree", rationale="a"
        ),
        IndexCandidate(
            table="orders", columns=["status"], index_type="btree", rationale="b"
        ),
        IndexCandidate(
            table="orders",
            columns=["created_at"],
            index_type="btree",
            rationale="c",
        ),
    ]
    conn = _make_conn_multi(cost_before=5000.0, cost_after=50.0)
    result = asyncio.run(
        validate_candidates_combined(candidates, "SELECT * FROM orders", conn)
    )
    assert result.improvement_pct == pytest.approx((5000.0 - 50.0) / 5000.0)
    assert isinstance(result, CombinedValidationResult)


def test_combined_validation_drops_all_indexes_on_success():
    """hypopg_drop_index is called once per candidate even on success."""
    candidates = [
        IndexCandidate(
            table="orders", columns=["col1"], index_type="btree", rationale="x"
        ),
        IndexCandidate(
            table="orders", columns=["col2"], index_type="btree", rationale="y"
        ),
    ]
    conn = _make_conn_multi(cost_before=1000.0, cost_after=100.0, indexrelid_start=20)

    asyncio.run(validate_candidates_combined(candidates, "SELECT 1", conn))

    # execute should have been called once per indexrelid (2 candidates → 2 drops)
    assert conn.execute.await_count == 2


def test_combined_validation_drops_all_indexes_on_explain_failure():
    """All hypothetical indexes are dropped even when the second EXPLAIN raises."""
    candidates = [
        IndexCandidate(
            table="orders", columns=["col1"], index_type="btree", rationale="x"
        ),
        IndexCandidate(
            table="orders", columns=["col2"], index_type="btree", rationale="y"
        ),
    ]

    conn = AsyncMock()
    call_count_fetchrow = 0

    async def _fetchrow(query, *args, **kwargs):
        nonlocal call_count_fetchrow
        call_count_fetchrow += 1
        row = MagicMock()
        row.__getitem__ = lambda self, key: (
            30 + call_count_fetchrow - 1 if key == "indexrelid" else None
        )
        return row

    conn.fetchrow = _fetchrow
    conn.execute = AsyncMock(return_value=None)

    call_count_fetchval = 0

    async def _fetchval(query, *args, **kwargs):
        nonlocal call_count_fetchval
        call_count_fetchval += 1
        if call_count_fetchval == 1:
            return json.dumps([_make_plan(1000.0)])
        raise RuntimeError("Simulated EXPLAIN failure")

    conn.fetchval = _fetchval

    with pytest.raises(RuntimeError, match="Simulated EXPLAIN failure"):
        asyncio.run(validate_candidates_combined(candidates, "SELECT 1", conn))

    # Both indexes must be cleaned up
    assert conn.execute.await_count == 2


def test_combined_validation_zero_cost_before():
    """When cost_before is 0, improvement_pct must be 0.0."""
    conn = _make_conn_multi(cost_before=0.0, cost_after=0.0)
    result = asyncio.run(validate_candidates_combined([], "SELECT 1", conn))
    assert result.improvement_pct == pytest.approx(0.0)


def test_combined_validation_result_new_plan_is_after_plan():
    """new_plan must reflect the cost from the second EXPLAIN (after indexes)."""
    cand = IndexCandidate(
        table="orders", columns=["user_id"], index_type="btree", rationale="test"
    )
    conn = _make_conn_multi(cost_before=2000.0, cost_after=20.0)
    result = asyncio.run(validate_candidates_combined([cand], "SELECT 1", conn))
    assert result.new_plan["Plan"]["Total Cost"] == pytest.approx(20.0)


# ---------------------------------------------------------------------------
# _detect_redundant_recommendations
# ---------------------------------------------------------------------------


def test_detect_redundant_single_col_subset_of_composite():
    """A single-column rec that is a strict subset of a composite rec gets flagged."""
    composite = _make_rec("orders", ["user_id", "status"], 0.9)
    single = _make_rec("orders", ["user_id"], 0.5)
    recs = [composite, single]

    _detect_redundant_recommendations(recs)

    assert composite.notes == []
    assert len(single.notes) == 1
    assert "Potentially redundant" in single.notes[0]
    assert "user_id" in single.notes[0]
    assert "status" in single.notes[0]


def test_detect_redundant_no_overlap():
    """Two recs with different columns on the same table are not flagged."""
    rec_a = _make_rec("orders", ["user_id"], 0.9)
    rec_b = _make_rec("orders", ["status"], 0.5)

    _detect_redundant_recommendations([rec_a, rec_b])

    assert rec_a.notes == []
    assert rec_b.notes == []


def test_detect_redundant_different_tables():
    """A rec that is a column-subset of another on a different table is not flagged."""
    rec_a = _make_rec("orders", ["user_id", "status"], 0.9)
    rec_b = _make_rec("users", ["user_id"], 0.5)  # different table

    _detect_redundant_recommendations([rec_a, rec_b])

    assert rec_a.notes == []
    assert rec_b.notes == []


def test_detect_redundant_equal_columns_not_flagged():
    """Two recs with exactly the same columns (equal sets) are not flagged."""
    rec_a = _make_rec("orders", ["user_id", "status"], 0.9)
    rec_b = _make_rec("orders", ["user_id", "status"], 0.5)

    _detect_redundant_recommendations([rec_a, rec_b])

    assert rec_a.notes == []
    assert rec_b.notes == []


def test_detect_redundant_only_adds_note_once():
    """Even if there are two composites that cover the single-column rec,
    only one 'potentially redundant' note is added."""
    composite1 = _make_rec("orders", ["user_id", "status"], 0.9)
    composite2 = _make_rec("orders", ["user_id", "created_at"], 0.8)
    single = _make_rec("orders", ["user_id"], 0.5)

    _detect_redundant_recommendations([composite1, composite2, single])

    # single.notes should have exactly one entry
    assert len(single.notes) == 1
    assert "Potentially redundant" in single.notes[0]


def test_detect_redundant_multiple_subsets():
    """Each subset rec is flagged independently."""
    composite = _make_rec("orders", ["a", "b", "c"], 0.9)
    sub1 = _make_rec("orders", ["a"], 0.5)
    sub2 = _make_rec("orders", ["a", "b"], 0.4)

    _detect_redundant_recommendations([composite, sub1, sub2])

    assert len(sub1.notes) == 1
    assert len(sub2.notes) == 1
    assert composite.notes == []


# ---------------------------------------------------------------------------
# Ranking behaviour
# ---------------------------------------------------------------------------


def test_recommendations_ranked_by_improvement_pct(capsys):
    """_print_recommendations shows recs ordered by improvement_pct descending."""
    from pgreviewer.cli.commands.check import _print_recommendations

    recs = [
        IndexRecommendation(
            table="orders",
            columns=["status"],
            index_type="btree",
            create_statement="CREATE INDEX ... ON orders(status);",
            cost_before=100.0,
            cost_after=60.0,
            improvement_pct=0.40,
            validated=True,
        ),
        IndexRecommendation(
            table="orders",
            columns=["user_id"],
            index_type="btree",
            create_statement="CREATE INDEX ... ON orders(user_id);",
            cost_before=100.0,
            cost_after=5.0,
            improvement_pct=0.95,
            validated=True,
        ),
        IndexRecommendation(
            table="orders",
            columns=["created_at"],
            index_type="btree",
            create_statement="CREATE INDEX ... ON orders(created_at);",
            cost_before=100.0,
            cost_after=70.0,
            improvement_pct=0.30,
            validated=False,
        ),
    ]

    _print_recommendations(recs)
    captured = capsys.readouterr()

    # Verify all three are printed
    assert "orders(user_id)" in captured.out
    assert "orders(status)" in captured.out
    assert "orders(created_at)" in captured.out
    # No diminishing-returns warning for exactly 3 recommendations
    assert "diminishing" not in captured.out


# ---------------------------------------------------------------------------
# Diminishing returns warning
# ---------------------------------------------------------------------------


def test_diminishing_returns_warning_shown_for_more_than_3(capsys):
    """_print_recommendations prints the diminishing returns warning when >3 recs."""
    from pgreviewer.cli.commands.check import _print_recommendations

    recs = [
        IndexRecommendation(
            table="t",
            columns=[f"col{i}"],
            index_type="btree",
            create_statement=f"CREATE INDEX ... ON t(col{i});",
            improvement_pct=0.5 - i * 0.05,
            validated=True,
        )
        for i in range(4)
    ]

    _print_recommendations(recs)
    captured = capsys.readouterr()

    assert "diminishing" in captured.out
    assert "write-performance returns" in captured.out


def test_diminishing_returns_warning_not_shown_for_3_or_fewer(capsys):
    """_print_recommendations does NOT print the warning when there are ≤3 recs."""
    from pgreviewer.cli.commands.check import _print_recommendations

    recs = [
        IndexRecommendation(
            table="t",
            columns=[f"col{i}"],
            index_type="btree",
            create_statement=f"CREATE INDEX ... ON t(col{i});",
            improvement_pct=0.9 - i * 0.1,
            validated=True,
        )
        for i in range(3)
    ]

    _print_recommendations(recs)
    captured = capsys.readouterr()

    assert "diminishing" not in captured.out


# ---------------------------------------------------------------------------
# Redundancy notes in output
# ---------------------------------------------------------------------------


def test_print_recommendations_shows_redundancy_note(capsys):
    """_print_recommendations displays 'Potentially redundant' notes."""
    from pgreviewer.cli.commands.check import _print_recommendations

    rec = IndexRecommendation(
        table="orders",
        columns=["user_id"],
        index_type="btree",
        create_statement="CREATE INDEX ... ON orders(user_id);",
        improvement_pct=0.5,
        validated=True,
        notes=[
            "Potentially redundant: columns are a subset of the "
            "composite index on (user_id, status)"
        ],
    )

    _print_recommendations([rec])
    captured = capsys.readouterr()

    assert "Potentially redundant" in captured.out
    assert "user_id, status" in captured.out
