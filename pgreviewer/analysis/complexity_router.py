from pgreviewer.analysis.plan_parser import walk_nodes
from pgreviewer.config import settings
from pgreviewer.core.models import ExplainPlan, Issue

_JOIN_NODE_TYPES = {"Nested Loop", "Hash Join", "Merge Join"}
_CLEAR_ACTION_CONFIDENCE = 0.8


def should_use_llm(plan: ExplainPlan, issues: list[Issue]) -> tuple[bool, str]:
    join_count = 0

    for node in walk_nodes(plan):
        if node.node_type in _JOIN_NODE_TYPES:
            join_count += 1
        if node.node_type == "CTE Scan":
            return True, "contains CTE"
        if node.node_type == "InitPlan":
            return True, "contains InitPlan"
        if node.node_type == "SubPlan":
            return True, "contains subquery"

    if join_count >= 3:
        return True, "3+ joins"

    if issues and all(issue.confidence < _CLEAR_ACTION_CONFIDENCE for issue in issues):
        return True, "low-confidence issues"

    if plan.root.total_cost > settings.HIGH_COST_THRESHOLD and len(issues) < 2:
        return True, "high cost with few detector hits"

    return False, "simple plan"
