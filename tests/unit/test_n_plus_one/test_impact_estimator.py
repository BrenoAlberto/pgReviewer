from pgreviewer.analysis.code_pattern_detectors.base import QueryCatalog
from pgreviewer.analysis.code_pattern_detectors.query_in_loop import QueryInLoopDetector
from pgreviewer.analysis.impact_estimator import estimate_loop_impact
from pgreviewer.core.models import SchemaInfo, TableInfo

from .conftest import parse_python_source


def test_impact_estimator_correlates_source_table_row_count() -> None:
    detector = QueryInLoopDetector()
    parsed_file = parse_python_source(
        "app/service.py",
        "users = session.query(User).all()\n"
        "for user in users:\n"
        '    cursor.execute("SELECT * FROM orders WHERE user_id = %s", (user.id,))\n',
    )

    issues = detector.detect([parsed_file], QueryCatalog())
    estimate = estimate_loop_impact(
        issues[0],
        SchemaInfo(tables={"users": TableInfo(row_estimate=10_000)}),
    )

    assert estimate.source_table == "users"
    assert estimate.estimated_extra_queries == 10_000
    assert "~10 seconds of DB time" in estimate.summary


def test_impact_estimator_marks_unknown_without_source_table() -> None:
    detector = QueryInLoopDetector()
    parsed_file = parse_python_source(
        "app/direct.py",
        "def run(cursor, users):\n"
        "    for user in users:\n"
        '        cursor.execute("SELECT 1")\n',
    )

    issues = detector.detect([parsed_file], QueryCatalog())
    estimate = estimate_loop_impact(issues[0], SchemaInfo())

    assert estimate.requires_manual_review is True
    assert estimate.summary == "unknown iteration count — review manually"
