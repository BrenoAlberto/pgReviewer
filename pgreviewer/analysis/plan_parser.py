from collections.abc import Iterator
from typing import Any

from pgreviewer.core.models import ExplainPlan, PlanNode


def parse_explain(raw: dict[str, Any]) -> ExplainPlan:
    """
    Parses a raw EXPLAIN JSON dictionary into an ExplainPlan Pydantic model.
    The input is expected to be a single entry from the Postgres EXPLAIN array,
    containing a "Plan" key.
    """
    if "Plan" not in raw:
        # Check if it's the wrapped list
        if isinstance(raw, list) and len(raw) > 0 and "Plan" in raw[0]:
            raw = raw[0]
        else:
            raise ValueError("Invalid EXPLAIN format: 'Plan' key not found.")

    root_node = raw["Plan"]
    planning_time = raw.get("Planning Time")
    execution_time = raw.get("Execution Time")

    return ExplainPlan(
        root=PlanNode.model_validate(root_node),
        planning_time=planning_time,
        execution_time=execution_time,
    )


def walk_nodes(plan: ExplainPlan | PlanNode) -> Iterator[PlanNode]:
    """
    Depth-first traversal of the plan tree.
    """
    node = plan.root if isinstance(plan, ExplainPlan) else plan

    yield node
    for child in node.children:
        yield from walk_nodes(child)
