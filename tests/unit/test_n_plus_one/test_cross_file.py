from pgreviewer.analysis.code_pattern_detectors.python.query_in_loop import QueryInLoopDetector
from pgreviewer.analysis.query_catalog import QueryCatalog, build_catalog
from pgreviewer.core.models import Severity

from .conftest import parse_python_path, parse_python_source


def test_detects_cataloged_query_function_across_files(fixture_project) -> None:
    detector = QueryInLoopDetector()
    parsed_file = parse_python_path(fixture_project / "service_cross_file.py")
    catalog = build_catalog(fixture_project, force_rebuild=True)

    issues = detector.detect([parsed_file], catalog)

    assert len(issues) == 1
    issue = issues[0]
    assert issue.severity == Severity.CRITICAL
    assert issue.context["method_name"] == "get_by_id"
    assert issue.context["catalog_matches"] == ["repository.UserRepository.get_by_id"]
    assert issue.context["call_chain"]["query"]["file"] == "repository.py"


def test_does_not_flag_non_cataloged_cross_file_call() -> None:
    detector = QueryInLoopDetector()
    parsed_file = parse_python_source(
        "app/service.py",
        "def enrich_users(user_service, users):\n"
        "    for user in users:\n"
        "        user_service.format_name(user)\n",
    )

    issues = detector.detect([parsed_file], QueryCatalog())

    assert issues == []
