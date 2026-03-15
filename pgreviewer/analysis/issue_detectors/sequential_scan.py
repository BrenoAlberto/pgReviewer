from pgreviewer.analysis.issue_detectors import BaseDetector
from pgreviewer.core.models import ExplainPlan, Issue, SchemaInfo


class SequentialScanDetector(BaseDetector):
    """
    Stub detector for sequential scans.
    """

    @property
    def name(self) -> str:
        return "sequential_scan"

    def detect(self, plan: ExplainPlan, schema: SchemaInfo) -> list[Issue]:
        # Implementation will come in 1.2.4
        return []
