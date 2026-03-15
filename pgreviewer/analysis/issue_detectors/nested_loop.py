from pgreviewer.analysis.issue_detectors import BaseDetector
from pgreviewer.analysis.plan_parser import walk_nodes
from pgreviewer.config import settings
from pgreviewer.core.models import (
    ExplainPlan,
    Issue,
    PlanNode,
    SchemaInfo,
    Severity,
)

_CRITICAL_OUTER_THRESHOLD = 100_000


def _get_relation_name(node: PlanNode) -> str | None:
    """Return the relation name for *node* or its first descendant that has one.

    Preference order: ``relation_name`` (the physical table name) takes priority
    over ``alias_name``; if neither is set on *node*, the search descends into
    children depth-first until a non-empty name is found.
    """
    if node.relation_name:
        return node.relation_name
    if node.alias_name:
        return node.alias_name
    for child in node.children:
        name = _get_relation_name(child)
        if name:
            return name
    return None


class NestedLoopLargeOuterDetector(BaseDetector):
    """
    Detects Nested Loop join nodes where the outer relation has a large
    estimated row count, which can lead to O(n²) execution.

    Severity is HIGH when the outer relation has between
    ``settings.NESTED_LOOP_OUTER_THRESHOLD`` and ``_CRITICAL_OUTER_THRESHOLD``
    rows, and CRITICAL above ``_CRITICAL_OUTER_THRESHOLD``.
    """

    @property
    def name(self) -> str:
        return "nested_loop_large_outer"

    def detect(self, plan: ExplainPlan, schema: SchemaInfo) -> list[Issue]:
        issues: list[Issue] = []

        for node in walk_nodes(plan):
            if node.node_type != "Nested Loop":
                continue
            if len(node.children) < 2:
                continue

            outer = node.children[0]
            inner = node.children[1]

            if outer.plan_rows <= settings.NESTED_LOOP_OUTER_THRESHOLD:
                continue

            if outer.plan_rows > _CRITICAL_OUTER_THRESHOLD:
                severity = Severity.CRITICAL
            else:
                severity = Severity.WARNING

            outer_table = _get_relation_name(outer) or "unknown"
            inner_table = _get_relation_name(inner) or "unknown"

            issues.append(
                Issue(
                    detector_name=self.name,
                    severity=severity,
                    description=(
                        f"Nested loop join with large outer relation '{outer_table}' "
                        f"({outer.plan_rows:,} estimated rows) joining '{inner_table}'"
                    ),
                    affected_table=outer_table,
                    affected_columns=[],
                    suggested_action=(
                        "Consider a Hash Join or ensure an index exists on "
                        "the inner relation's join column"
                    ),
                    context={
                        "inner_table": inner_table,
                        "outer_rows": outer.plan_rows,
                    },
                )
            )

        return issues
