from pgreviewer.analysis.issue_detectors import BaseDetector
from pgreviewer.config import settings
from pgreviewer.core.models import ExplainPlan, Issue, SchemaInfo, Severity


class HighCostDetector(BaseDetector):
    """
    Detects queries where the total estimated cost exceeds a configured threshold.
    This serves as a general indicator of complex or inefficient queries.
    """

    @property
    def name(self) -> str:
        return "high_cost"

    def detect(self, plan: ExplainPlan, schema: SchemaInfo) -> list[Issue]:
        issues: list[Issue] = []
        root_node = plan.root

        if root_node.total_cost > settings.HIGH_COST_THRESHOLD:
            issues.append(
                Issue(
                    detector_name=self.name,
                    severity=Severity.WARNING,
                    description=(
                        f"Query total cost ({root_node.total_cost:,.2f}) exceeds "
                        f"the threshold of {settings.HIGH_COST_THRESHOLD:,.2f}"
                    ),
                    affected_table=None,
                    affected_columns=[],
                    suggested_action=(
                        "Review the query plan for expensive nodes such as large "
                        "sequential scans or inefficient joins."
                    ),
                    context={
                        "total_cost": root_node.total_cost,
                        "threshold": settings.HIGH_COST_THRESHOLD,
                    },
                )
            )

        return issues
