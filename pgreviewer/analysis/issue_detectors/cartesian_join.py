from pgreviewer.analysis.issue_detectors import BaseDetector
from pgreviewer.analysis.plan_parser import walk_nodes
from pgreviewer.core.models import (
    ExplainPlan,
    Issue,
    IssueSeverity,
    PlanNode,
    SchemaInfo,
)


def _get_tables_in_node(node: PlanNode) -> list[str]:
    tables = []
    for child in walk_nodes(node):
        name = child.relation_name or child.alias_name
        if name:
            tables.append(name)
    # unique preserving order
    return list(dict.fromkeys(tables))


class CartesianJoinDetector(BaseDetector):
    """
    Detects any Join node (Nested Loop, Hash Join, Merge Join) with no join condition.
    """

    @property
    def name(self) -> str:
        return "cartesian_join"

    def detect(self, plan: ExplainPlan, schema: SchemaInfo) -> list[Issue]:
        issues: list[Issue] = []

        for node in walk_nodes(plan):
            if node.node_type not in ("Nested Loop", "Hash Join", "Merge Join"):
                continue

            # check for condition
            if node.hash_cond or node.merge_cond or node.join_filter:
                continue

            # Check if any child has an index_cond matching tables inside children.
            # A cartesian join is genuinely a missing condition. But nested loops can
            # push down conditions into 'Index Cond' of the inner child.
            has_pushed_cond = False
            if node.node_type == "Nested Loop":
                for child in walk_nodes(node):
                    if child.index_cond:
                        # Naive heuristic: if it has an Index Cond,
                        # it might be parameterized.
                        has_pushed_cond = True
                        break

            if has_pushed_cond:
                continue

            tables = _get_tables_in_node(node)
            tables_str = ", ".join(tables) if tables else "unknown tables"

            issues.append(
                Issue(
                    detector_name=self.name,
                    severity=IssueSeverity.CRITICAL,
                    message=(
                        f"Cartesian Join detected between {tables_str}. A join "
                        "without a condition explodes row count multiplicatively."
                    ),
                    context={
                        "tables": tables,
                        "node_type": node.node_type,
                    },
                )
            )

        return issues
