import json
from pathlib import Path

import pytest

from pgreviewer.analysis.issue_detectors.sort_without_index import (
    SortWithoutIndexDetector,
)
from pgreviewer.analysis.plan_parser import parse_explain
from pgreviewer.core.models import (
    ExplainPlan,
    IndexInfo,
    PlanNode,
    SchemaInfo,
    Severity,
    TableInfo,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "explain"


def _load_plan(fixture_name: str) -> ExplainPlan:
    with open(FIXTURE_DIR / fixture_name) as f:
        raw = json.load(f)
    return parse_explain(raw[0])


def test_sort_without_index_flags_missing_index():
    # Setup: Sort on 'orders' (created_at) with 2000 rows, no index
    scan_node = PlanNode(
        node_type="Seq Scan",
        relation_name="orders",
        total_cost=100.0,
        startup_cost=0.0,
        plan_rows=2000,
        plan_width=10,
        children=[],
    )
    sort_node = PlanNode(
        node_type="Sort",
        sort_key=["created_at"],
        total_cost=200.0,
        startup_cost=150.0,
        plan_rows=2000,
        plan_width=10,
        children=[scan_node],
    )
    plan = ExplainPlan(root=sort_node)
    schema = SchemaInfo()

    detector = SortWithoutIndexDetector()
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    assert issues[0].severity == Severity.WARNING
    assert "orders" in issues[0].description
    assert "created_at" in issues[0].description


def test_sort_without_index_ignores_small_sorts():
    # Setup: Sort on 'orders' with 500 rows
    scan_node = PlanNode(
        node_type="Seq Scan",
        relation_name="orders",
        total_cost=100.0,
        startup_cost=0.0,
        plan_rows=500,
        plan_width=10,
        children=[],
    )
    sort_node = PlanNode(
        node_type="Sort",
        sort_key=["created_at"],
        total_cost=110.0,
        startup_cost=105.0,
        plan_rows=500,
        plan_width=10,
        children=[scan_node],
    )
    plan = ExplainPlan(root=sort_node)
    schema = SchemaInfo()

    detector = SortWithoutIndexDetector()
    issues = detector.detect(plan, schema)

    assert len(issues) == 0


def test_sort_without_index_ignores_when_index_exists():
    # Setup: Sort on 'orders'(created_at), index exists
    scan_node = PlanNode(
        node_type="Seq Scan",
        relation_name="orders",
        total_cost=100.0,
        startup_cost=0.0,
        plan_rows=2000,
        plan_width=10,
        children=[],
    )
    sort_node = PlanNode(
        node_type="Sort",
        sort_key=["created_at"],
        total_cost=200.0,
        startup_cost=150.0,
        plan_rows=2000,
        plan_width=10,
        children=[scan_node],
    )
    plan = ExplainPlan(root=sort_node)
    schema = SchemaInfo(
        tables={
            "orders": TableInfo(
                indexes=[
                    IndexInfo(name="idx_orders_created_at", columns=["created_at"]),
                ]
            ),
        }
    )

    detector = SortWithoutIndexDetector()
    issues = detector.detect(plan, schema)

    assert len(issues) == 0


def test_sort_without_index_handles_complex_sort_keys():
    # Setup: Sort key with ASC/DESC and table alias
    scan_node = PlanNode(
        node_type="Seq Scan",
        relation_name="orders",
        alias_name="o",
        total_cost=100.0,
        startup_cost=0.0,
        plan_rows=2000,
        plan_width=10,
        children=[],
    )
    sort_node = PlanNode(
        node_type="Sort",
        sort_key=["o.created_at DESC NULLS LAST"],
        total_cost=200.0,
        startup_cost=150.0,
        plan_rows=2000,
        plan_width=10,
        children=[scan_node],
    )
    plan = ExplainPlan(root=sort_node)
    schema = SchemaInfo()

    detector = SortWithoutIndexDetector()
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    assert "created_at" in str(issues[0].affected_columns)


# ---------------------------------------------------------------------------
# Fixture-based tests: one plan that triggers, one that does not
# ---------------------------------------------------------------------------


def test_sort_without_index_fixture_triggers():
    """sort_without_index.json must trigger the detector (500K-row Sort)."""
    plan = _load_plan("sort_without_index.json")
    schema = SchemaInfo()

    detector = SortWithoutIndexDetector()
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    issue = issues[0]
    assert issue.severity == Severity.WARNING
    assert issue.affected_table == "orders"
    assert "created_at" in issue.affected_columns


def test_index_scan_fixture_no_sort_no_issue():
    """index_scan.json has no Sort node — the detector must emit nothing."""
    plan = _load_plan("index_scan.json")
    schema = SchemaInfo()

    detector = SortWithoutIndexDetector()
    issues = detector.detect(plan, schema)

    assert issues == []


def test_sort_suppressed_when_index_covers_fixture_columns():
    """When schema contains a covering index, sort_without_index.json must
    not produce any issue."""
    plan = _load_plan("sort_without_index.json")
    schema = SchemaInfo(
        tables={
            "orders": TableInfo(
                indexes=[
                    IndexInfo(
                        name="idx_orders_created_at", columns=["created_at"]
                    ),
                ]
            )
        }
    )

    detector = SortWithoutIndexDetector()
    issues = detector.detect(plan, schema)

    assert issues == []


# ---------------------------------------------------------------------------
# Threshold-boundary tests via pytest.mark.parametrize
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "input_rows,should_flag",
    [
        (500, False),   # well below the 1,000-row threshold — not flagged
        (1000, False),  # exactly at threshold (boundary, exclusive) — not flagged
        (1001, True),   # just above threshold — flagged
        (50_000, True), # large input — flagged
    ],
)
def test_sort_threshold_boundary(input_rows: int, should_flag: bool):
    """Detector must flag Sorts only when input rows strictly exceed 1 000."""
    scan_node = PlanNode(
        node_type="Seq Scan",
        relation_name="orders",
        total_cost=float(input_rows * 0.1),
        startup_cost=0.0,
        plan_rows=input_rows,
        plan_width=10,
        children=[],
    )
    sort_node = PlanNode(
        node_type="Sort",
        sort_key=["created_at"],
        total_cost=float(input_rows * 0.2),
        startup_cost=float(input_rows * 0.15),
        plan_rows=input_rows,
        plan_width=10,
        children=[scan_node],
    )
    plan = ExplainPlan(root=sort_node)
    schema = SchemaInfo()

    issues = SortWithoutIndexDetector().detect(plan, schema)

    if should_flag:
        assert len(issues) == 1
    else:
        assert issues == []
