from pgreviewer.analysis.code_pattern_detectors.base import QueryCatalog
from pgreviewer.analysis.code_pattern_detectors.python.query_in_loop import (
    QueryInLoopDetector,
)
from pgreviewer.core.models import Severity

from .conftest import parse_python_path


def test_suppression_comment_and_small_range_heuristic(fixture_project) -> None:
    detector = QueryInLoopDetector()
    parsed_file = parse_python_path(fixture_project / "service_false_positive.py")

    issues = detector.detect([parsed_file], QueryCatalog())

    assert len(issues) == 1
    assert issues[0].severity == Severity.INFO
    assert issues[0].context["iterable"] == "range(3)"
    assert detector.suppressed_findings
    assert detector.suppressed_findings[0]["reason"] == "inline_comment"
