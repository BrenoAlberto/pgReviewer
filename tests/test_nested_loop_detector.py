import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pgreviewer.analysis.issue_detectors.nested_loop import NestedLoopLargeOuterDetector
from pgreviewer.analysis.plan_parser import parse_explain
from pgreviewer.core.models import SchemaInfo, Severity

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "explain"


def _load_plan(fixture_name: str):
    with open(FIXTURE_DIR / fixture_name) as f:
        raw = json.load(f)
    return parse_explain(raw[0])


@pytest.fixture
def detector():
    return NestedLoopLargeOuterDetector()


@pytest.fixture
def schema():
    return SchemaInfo()


def test_large_outer_critical(detector, schema):
    """Nested loop with 500K outer rows (> 100K) must produce a CRITICAL issue."""
    plan = _load_plan("nested_loop.json")
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    issue = issues[0]
    assert issue.severity == Severity.CRITICAL
    assert issue.detector_name == "nested_loop_large_outer"
    assert issue.affected_table == "orders"
    assert issue.context["outer_rows"] == 500_000
    assert "hash join" in issue.suggested_action.lower()


def test_large_outer_warning(detector, schema):
    """Nested loop 50K outer rows (1K >= rows <= 100K) must produce a HIGH issue."""
    plan = _load_plan("nested_loop_warning.json")
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    issue = issues[0]
    assert issue.severity == Severity.WARNING
    assert issue.detector_name == "nested_loop_large_outer"
    assert issue.affected_table == "orders"
    assert issue.context["outer_rows"] == 50_000
    assert "hash join" in issue.suggested_action.lower()


def test_small_outer_no_issue(detector, schema):
    """Nested loop with 50 outer rows (below threshold) must not produce any issue."""
    plan = _load_plan("nested_loop_small.json")
    issues = detector.detect(plan, schema)

    assert issues == []


def test_threshold_respected(detector, schema):
    """Outer rows equal to the threshold must not raise an issue.

    Detection uses ``outer.plan_rows <= threshold``, so a value exactly at
    the threshold is intentionally excluded.
    """
    plan = _load_plan("nested_loop_warning.json")
    with patch(
        "pgreviewer.analysis.issue_detectors.nested_loop.settings"
    ) as mock_settings:
        mock_settings.NESTED_LOOP_OUTER_THRESHOLD = 50_000
        issues = detector.detect(plan, schema)

    assert issues == []


def test_inner_table_included_in_context(detector, schema):
    """The inner table name must be captured in the issue context."""
    plan = _load_plan("nested_loop_warning.json")
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    assert issues[0].context["inner_table"] == "users"


def test_large_outer_critical_dedicated_fixture(detector, schema):
    """nested_loop_large_outer.json must also trigger a CRITICAL issue."""
    plan = _load_plan("nested_loop_large_outer.json")
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    issue = issues[0]
    assert issue.severity == Severity.CRITICAL
    assert issue.affected_table == "orders"
    assert issue.context["outer_rows"] == 500_000


def test_nested_loop_small_fixture_no_issue(detector, schema):
    """nested_loop_small.json must not trigger the detector."""
    plan = _load_plan("nested_loop_small.json")
    issues = detector.detect(plan, schema)

    assert issues == []


# ---------------------------------------------------------------------------
# Severity threshold boundary tests via pytest.mark.parametrize
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "outer_rows,expected_severity",
    [
        (500, None),                 # below threshold (1K) → no issue
        (1_000, None),               # exactly at threshold (exclusive) → no issue
        (1_001, Severity.WARNING),   # just above threshold → WARNING
        (50_000, Severity.WARNING),  # well above threshold, below critical
        (100_000, Severity.WARNING), # exactly at critical boundary → WARNING
        (100_001, Severity.CRITICAL), # just above critical → CRITICAL
        (500_000, Severity.CRITICAL), # well above critical → CRITICAL
    ],
)
def test_nested_loop_severity_boundaries(
    outer_rows, expected_severity, detector, schema
):
    """Severity must follow the documented threshold boundaries exactly."""
    from pgreviewer.core.models import ExplainPlan, PlanNode

    outer = PlanNode(
        node_type="Seq Scan",
        relation_name="orders",
        total_cost=float(outer_rows),
        startup_cost=0.0,
        plan_rows=outer_rows,
        plan_width=10,
        children=[],
    )
    inner = PlanNode(
        node_type="Index Scan",
        relation_name="users",
        index_name="users_pkey",
        index_cond="(users.id = orders.user_id)",
        total_cost=1.0,
        startup_cost=0.0,
        plan_rows=1,
        plan_width=10,
        children=[],
    )
    nl_node = PlanNode(
        node_type="Nested Loop",
        join_type="Inner",
        total_cost=float(outer_rows * 2),
        startup_cost=0.0,
        plan_rows=outer_rows,
        plan_width=20,
        children=[outer, inner],
    )
    plan = ExplainPlan(root=nl_node)

    issues = detector.detect(plan, schema)

    if expected_severity is None:
        assert issues == []
    else:
        assert len(issues) == 1
        assert issues[0].severity == expected_severity
