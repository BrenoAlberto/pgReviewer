from pgreviewer.analysis.code_pattern_detectors.base import QueryCatalog
from pgreviewer.analysis.code_pattern_detectors.query_in_loop import QueryInLoopDetector
from pgreviewer.analysis.code_pattern_detectors.sqlalchemy_n_plus_one import (
    SQLAlchemyNPlusOneDetector,
)
from pgreviewer.analysis.query_catalog import build_catalog

from .conftest import parse_python_path


def test_fix_suggestion_for_direct_query_in_loop(fixture_project) -> None:
    detector = QueryInLoopDetector()
    parsed_file = parse_python_path(fixture_project / "service_direct.py")

    issues = detector.detect([parsed_file], QueryCatalog())

    assert "WHERE id = ANY(%s)" in issues[0].suggested_action
    assert "for user_id in user_ids" in issues[0].suggested_action


def test_fix_suggestion_for_cross_file_query_in_loop(fixture_project) -> None:
    detector = QueryInLoopDetector()
    parsed_file = parse_python_path(fixture_project / "service_cross_file.py")
    catalog = build_catalog(fixture_project, force_rebuild=True)

    issues = detector.detect([parsed_file], catalog)

    assert "WHERE id = ANY(%s)" in issues[0].suggested_action
    assert "for user in users" in issues[0].suggested_action


def test_fix_suggestion_for_sqlalchemy_n_plus_one(fixture_project) -> None:
    detector = SQLAlchemyNPlusOneDetector()
    files = [
        parse_python_path(fixture_project / "models.py"),
        parse_python_path(fixture_project / "service_lazy_load.py"),
    ]

    issues = detector.detect(files, QueryCatalog())

    assert "selectinload(User.orders)" in issues[0].suggested_action
