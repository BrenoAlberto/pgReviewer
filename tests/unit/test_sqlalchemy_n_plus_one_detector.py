from pathlib import Path

from pgreviewer.analysis.code_pattern_detectors.base import ParsedFile, QueryCatalog
from pgreviewer.analysis.code_pattern_detectors.sqlalchemy_n_plus_one import (
    SQLAlchemyNPlusOneDetector,
)
from pgreviewer.core.models import Severity
from pgreviewer.parsing.treesitter import TSParser

_FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "python_sources"


def _parsed_python_file(path: Path) -> ParsedFile:
    source = path.read_text(encoding="utf-8")
    parser = TSParser("python")
    return ParsedFile(
        path=str(path),
        tree=parser.parse_file(source, language="python"),
        language="python",
        content=source,
    )


def test_detects_lazy_relationship_access_inside_loop() -> None:
    detector = SQLAlchemyNPlusOneDetector()
    files = [
        _parsed_python_file(_FIXTURES_DIR / "sqlalchemy_n_plus_one_models.py"),
        _parsed_python_file(_FIXTURES_DIR / "sqlalchemy_n_plus_one_queries.py"),
    ]

    issues = detector.detect(files, QueryCatalog())
    relationship_issues = [
        issue for issue in issues if issue.context.get("relationship") == "orders"
    ]

    assert len(relationship_issues) == 1
    assert relationship_issues[0].severity == Severity.CRITICAL
    assert relationship_issues[0].detector_name == "sqlalchemy_n_plus_one"
    assert relationship_issues[0].context["loop_variable"] == "user"
    assert relationship_issues[0].context["iterable"] == "users"
    assert "```python" in relationship_issues[0].suggested_action
    assert "selectinload(User.orders)" in relationship_issues[0].suggested_action


def test_does_not_flag_column_access_on_loop_variable() -> None:
    detector = SQLAlchemyNPlusOneDetector()
    files = [
        _parsed_python_file(_FIXTURES_DIR / "sqlalchemy_n_plus_one_models.py"),
        _parsed_python_file(_FIXTURES_DIR / "sqlalchemy_n_plus_one_queries.py"),
    ]

    issues = detector.detect(files, QueryCatalog())

    assert all(issue.context.get("relationship") != "name" for issue in issues)


def test_does_not_flag_when_relationship_is_eager_loaded() -> None:
    detector = SQLAlchemyNPlusOneDetector()
    files = [
        _parsed_python_file(_FIXTURES_DIR / "sqlalchemy_n_plus_one_models.py"),
        _parsed_python_file(_FIXTURES_DIR / "sqlalchemy_n_plus_one_queries.py"),
    ]

    issues = detector.detect(files, QueryCatalog())

    eager_issue_lines = {
        issue.context["line_number"]
        for issue in issues
        if issue.context.get("relationship") == "orders"
    }
    assert 19 not in eager_issue_lines
