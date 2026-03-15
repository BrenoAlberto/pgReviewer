from pgreviewer.analysis.issue_detectors import BaseDetector
from pgreviewer.analysis.plan_parser import walk_nodes
from pgreviewer.config import settings
from pgreviewer.core.models import ExplainPlan, Issue, SchemaInfo, Severity

_CRITICAL_ROW_THRESHOLD = 1_000_000
_HIGH_ROW_THRESHOLD = 50_000


class SequentialScanDetector(BaseDetector):
    """
    Detects sequential scans on tables that exceed the configured row threshold.
    """

    @property
    def name(self) -> str:
        return "sequential_scan"

    def detect(self, plan: ExplainPlan, schema: SchemaInfo) -> list[Issue]:
        issues: list[Issue] = []
        for node in walk_nodes(plan):
            if node.node_type != "Seq Scan":
                continue
            if node.plan_rows <= settings.SEQ_SCAN_ROW_THRESHOLD:
                continue

            if node.plan_rows > _CRITICAL_ROW_THRESHOLD:
                severity = Severity.CRITICAL
            elif node.plan_rows > _HIGH_ROW_THRESHOLD:
                severity = Severity.WARNING
            else:
                severity = Severity.INFO

            table_name = node.relation_name or node.alias_name or "unknown"
            if node.filter_expr:
                suggested_action = "Consider adding an index on the filter columns"
            else:
                suggested_action = "Review if full table scan is intentional"
            issues.append(
                Issue(
                    detector_name=self.name,
                    severity=severity,
                    description=(
                        f"Sequential scan on table '{table_name}' "
                        f"with {node.plan_rows:,} estimated rows"
                    ),
                    affected_table=table_name,
                    affected_columns=[],
                    suggested_action=suggested_action,
                    context={
                        "estimated_rows": node.plan_rows,
                    },
                )
            )
        return issues
