from pgreviewer.analysis.complexity_router import should_use_llm
from pgreviewer.core.models import ExplainPlan, Issue, PlanNode, Severity


def _node(
    node_type: str,
    *,
    total_cost: float = 100.0,
    children: list[PlanNode] | None = None,
) -> PlanNode:
    return PlanNode(
        node_type=node_type,
        total_cost=total_cost,
        startup_cost=1.0,
        plan_rows=100,
        plan_width=32,
        children=children or [],
    )


def _issue(confidence: float = 1.0) -> Issue:
    return Issue(
        severity=Severity.WARNING,
        detector_name="detector",
        description="desc",
        affected_table=None,
        affected_columns=[],
        suggested_action="action",
        confidence=confidence,
    )


def test_should_use_llm_returns_false_for_simple_plan():
    plan = ExplainPlan(root=_node("Seq Scan"))

    assert should_use_llm(plan, []) == (False, "simple plan")


def test_should_use_llm_true_for_three_joins():
    plan = ExplainPlan(
        root=_node(
            "Nested Loop",
            children=[
                _node("Hash Join"),
                _node("Merge Join"),
            ],
        )
    )

    assert should_use_llm(plan, []) == (True, "3+ joins")


def test_should_use_llm_true_for_cte_scan():
    plan = ExplainPlan(root=_node("CTE Scan"))

    assert should_use_llm(plan, []) == (True, "contains cte")


def test_should_use_llm_true_for_init_plan():
    plan = ExplainPlan(root=_node("InitPlan"))

    assert should_use_llm(plan, []) == (True, "contains cte")


def test_should_use_llm_true_for_subplan():
    plan = ExplainPlan(root=_node("SubPlan"))

    assert should_use_llm(plan, []) == (True, "contains subquery")


def test_should_use_llm_true_for_low_confidence_issues():
    plan = ExplainPlan(root=_node("Seq Scan"))

    assert should_use_llm(plan, [_issue(0.5)]) == (True, "low-confidence issues")


def test_should_use_llm_true_for_high_cost_with_few_detector_hits():
    plan = ExplainPlan(root=_node("Seq Scan", total_cost=100_000.0))

    assert should_use_llm(plan, [_issue(1.0)]) == (
        True,
        "high cost with few detector hits",
    )
