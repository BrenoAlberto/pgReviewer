from pgreviewer.analysis.code_pattern_detectors.base import QueryCatalog
from pgreviewer.analysis.code_pattern_detectors.query_in_loop import QueryInLoopDetector
from pgreviewer.core.models import Severity

from .conftest import parse_python_path, parse_python_source


def test_detects_direct_query_in_loop_from_fixture(fixture_project) -> None:
    detector = QueryInLoopDetector()
    parsed_file = parse_python_path(fixture_project / "service_direct.py")

    issues = detector.detect([parsed_file], QueryCatalog())

    assert len(issues) == 1
    issue = issues[0]
    assert issue.severity == Severity.CRITICAL
    assert issue.context["method_name"] == "execute"
    assert issue.context["iterable"] == "user_ids"
    assert issue.context["query_text"] == "SELECT * FROM users WHERE id = %s"


def test_does_not_flag_loop_without_query_calls() -> None:
    detector = QueryInLoopDetector()
    parsed_file = parse_python_source(
        "app/non_query.py",
        "def run(users):\n"
        "    for user in users:\n"
        "        user.full_name()\n",
    )

    issues = detector.detect([parsed_file], QueryCatalog())

    assert issues == []
