from pgreviewer.analysis.issue_detectors.sort_without_index import (
    SortWithoutIndexDetector,
)
from pgreviewer.core.models import ExplainPlan, IssueSeverity, PlanNode, SchemaInfo


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
    schema = SchemaInfo(indexes={})

    detector = SortWithoutIndexDetector()
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    assert issues[0].severity == IssueSeverity.WARNING
    assert "orders" in issues[0].message
    assert "created_at" in issues[0].message


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
    schema = SchemaInfo(indexes={})

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
        indexes={
            "idx_orders_created_at": {"table": "orders", "columns": ["created_at"]}
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
    schema = SchemaInfo(indexes={})

    detector = SortWithoutIndexDetector()
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    assert "created_at" in str(issues[0].context["sort_columns"])
