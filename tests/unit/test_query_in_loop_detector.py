from pgreviewer.analysis.code_pattern_detectors.base import ParsedFile, QueryCatalog
from pgreviewer.analysis.code_pattern_detectors.query_in_loop import QueryInLoopDetector
from pgreviewer.config import settings
from pgreviewer.core.models import Severity
from pgreviewer.parsing.treesitter import TSParser


def _parsed_python_file(source: str) -> ParsedFile:
    parser = TSParser("python")
    return ParsedFile(
        path="app/example.py",
        tree=parser.parse_file(source, language="python"),
        language="python",
        content=source,
    )


def test_detects_direct_query_in_for_loop_with_query_text() -> None:
    detector = QueryInLoopDetector()
    parsed_file = _parsed_python_file(
        'for order in orders:\n    cursor.execute("SELECT * FROM orders")\n'
    )

    issues = detector.detect([parsed_file], QueryCatalog())

    assert len(issues) == 1
    issue = issues[0]
    assert issue.detector_name == "query_in_loop"
    assert issue.severity == Severity.CRITICAL
    assert issue.context["loop_variable"] == "order"
    assert issue.context["iterable"] == "orders"
    assert issue.context["query_text"] == "SELECT * FROM orders"


def test_detects_async_for_with_awaited_query_call() -> None:
    detector = QueryInLoopDetector()
    parsed_file = _parsed_python_file(
        "async def run(orders, conn):\n"
        "    async for order in orders:\n"
        '        await conn.fetch("SELECT * FROM orders WHERE id = $1", order)\n'
    )

    issues = detector.detect([parsed_file], QueryCatalog())

    assert len(issues) == 1
    assert issues[0].severity == Severity.CRITICAL
    assert issues[0].context["loop_variable"] == "order"
    assert issues[0].context["iterable"] == "orders"


def test_small_range_loop_is_warning() -> None:
    detector = QueryInLoopDetector()
    parsed_file = _parsed_python_file(
        'for i in range(3):\n    cursor.execute("SELECT 1")\n'
    )

    issues = detector.detect([parsed_file], QueryCatalog())

    assert len(issues) == 1
    assert issues[0].severity == Severity.WARNING


def test_query_methods_are_configurable(monkeypatch) -> None:
    monkeypatch.setattr(settings, "QUERY_METHODS", ["my_custom_db_method"])
    detector = QueryInLoopDetector()
    parsed_file = _parsed_python_file(
        "for user in users:\n    db.my_custom_db_method(user.id)\n"
    )

    issues = detector.detect([parsed_file], QueryCatalog())

    assert len(issues) == 1
    assert issues[0].context["method_name"] == "my_custom_db_method"
