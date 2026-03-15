from pgreviewer.analysis.issue_detectors.high_cost import HighCostDetector
from pgreviewer.config import settings
from pgreviewer.core.models import ExplainPlan, PlanNode, SchemaInfo, Severity


def test_high_cost_detector_flags_high_cost():
    # Setup test data with a high cost
    root_node = PlanNode(
        node_type="Result",
        total_cost=settings.HIGH_COST_THRESHOLD + 1,
        startup_cost=0.0,
        plan_rows=1,
        plan_width=1,
        children=[],
    )
    plan = ExplainPlan(root=root_node)
    schema = SchemaInfo()

    detector = HighCostDetector()
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    assert issues[0].severity == Severity.WARNING
    assert f"{settings.HIGH_COST_THRESHOLD:,.2f}" in issues[0].description
    assert f"{root_node.total_cost:,.2f}" in issues[0].description


def test_high_cost_detector_ignores_low_cost():
    # Setup test data with a low cost
    root_node = PlanNode(
        node_type="Result",
        total_cost=settings.HIGH_COST_THRESHOLD - 1,
        startup_cost=0.0,
        plan_rows=1,
        plan_width=1,
        children=[],
    )
    plan = ExplainPlan(root=root_node)
    schema = SchemaInfo()

    detector = HighCostDetector()
    issues = detector.detect(plan, schema)

    assert len(issues) == 0


def test_high_cost_detector_respects_custom_threshold(monkeypatch):
    # Mock settings to use a custom threshold
    custom_threshold = 500.0
    monkeypatch.setattr(settings, "HIGH_COST_THRESHOLD", custom_threshold)

    root_node = PlanNode(
        node_type="Result",
        total_cost=600.0,
        startup_cost=0.0,
        plan_rows=1,
        plan_width=1,
        children=[],
    )
    plan = ExplainPlan(root=root_node)
    schema = SchemaInfo()

    detector = HighCostDetector()
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    assert issues[0].severity == Severity.WARNING
    assert "600.00" in issues[0].description
    assert "500.00" in issues[0].description
