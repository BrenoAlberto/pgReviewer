from pgreviewer.analysis.code_pattern_detectors.python.query_in_loop import QueryInLoopDetector
from pgreviewer.analysis.query_catalog import build_catalog
from pgreviewer.core.models import Severity

from .conftest import parse_python_path


def test_detects_query_via_helper_call_chain(fixture_project) -> None:
    detector = QueryInLoopDetector()
    service_file = parse_python_path(fixture_project / "service_via_helper.py")
    helper_file = parse_python_path(fixture_project / "helper.py")
    catalog = build_catalog(fixture_project, force_rebuild=True)

    issues = detector.detect([service_file, helper_file], catalog)

    assert len(issues) == 1
    issue = issues[0]
    # users is a function parameter — unknown source → WARNING
    assert issue.severity == Severity.WARNING
    assert issue.context["method_name"] == "process_user"
    assert issue.context["call_chain"]["query"]["catalog_function"] == (
        "repository.UserRepository.get_by_id"
    )


def test_does_not_flag_when_call_chain_is_incomplete(fixture_project) -> None:
    detector = QueryInLoopDetector()
    service_file = parse_python_path(fixture_project / "service_via_helper.py")
    catalog = build_catalog(fixture_project, force_rebuild=True)

    issues = detector.detect([service_file], catalog)

    assert issues == []
