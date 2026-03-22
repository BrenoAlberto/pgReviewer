from pgreviewer.analysis.code_pattern_detectors.base import QueryCatalog
from pgreviewer.analysis.code_pattern_detectors.python.sqlalchemy_n_plus_one import (
    SQLAlchemyNPlusOneDetector,
)
from pgreviewer.core.models import Severity

from .conftest import parse_python_path


def test_detects_lazy_relationship_access(fixture_project) -> None:
    detector = SQLAlchemyNPlusOneDetector()
    files = [
        parse_python_path(fixture_project / "models.py"),
        parse_python_path(fixture_project / "service_lazy_load.py"),
    ]

    issues = detector.detect(files, QueryCatalog())
    relationship_issues = [
        issue for issue in issues if issue.context.get("relationship") == "orders"
    ]

    assert len(relationship_issues) == 1
    assert relationship_issues[0].severity == Severity.CRITICAL


def test_does_not_flag_eager_loaded_relationship(fixture_project) -> None:
    detector = SQLAlchemyNPlusOneDetector()
    files = [
        parse_python_path(fixture_project / "models.py"),
        parse_python_path(fixture_project / "service_eager_load.py"),
    ]

    issues = detector.detect(files, QueryCatalog())

    assert issues == []
