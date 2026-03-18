"""Unit tests for pgreviewer.parsing.file_classifier."""

from unittest.mock import patch

import pytest

from pgreviewer.parsing.file_classifier import FileType, classify_file

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EMPTY = ""
_PYTHON_WITH_OP_EXECUTE = "op.execute('SELECT 1')"
_PYTHON_WITH_CURSOR_EXECUTE = "cursor.execute('SELECT 1')"
_PYTHON_WITH_TEXT = "session.add(text('SELECT 1'))"
_PYTHON_WITH_SESSION_EXECUTE = "session.execute(query)"
_PYTHON_WITH_ORM_QUERY = "tasks = db.query(Task).filter(Task.project_id == pid).all()"
_PLAIN_PYTHON = "def hello():\n    return 42\n"
_SQLALCHEMY_MODEL = (
    "from sqlalchemy import Column, Integer, String\n"
    "from sqlalchemy.orm import relationship\n"
    "class User(Base):\n"
    "    __tablename__ = 'users'\n"
    "    id = Column(Integer, primary_key=True)\n"
)
_DECLARATIVE_BASE = "Base = declarative_base()\n"
_DECLARATIVE_BASE_V2 = "class Base(DeclarativeBase): pass\n"


# ---------------------------------------------------------------------------
# Migration SQL paths
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path",
    [
        "migrations/0001_init.sql",
        "db/migrations/0002_add_index.sql",
        "alembic/versions/001_baseline.sql",
        "flyway/V1__init.sql",
        "migrations/no_extension",
    ],
)
def test_migration_sql_paths(path):
    assert classify_file(path, _EMPTY) == FileType.MIGRATION_SQL


# ---------------------------------------------------------------------------
# Migration Python paths
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path",
    [
        "alembic/versions/001_add_index.py",
        "migrations/0003_drop_column.py",
        "flyway/migrate.py",
    ],
)
def test_migration_python_paths(path):
    assert classify_file(path, _EMPTY) == FileType.MIGRATION_PYTHON


# ---------------------------------------------------------------------------
# Raw SQL files (outside migration directories)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path",
    [
        "db/queries.sql",
        "queries.sql",
        "reports/monthly.sql",
    ],
)
def test_raw_sql_paths(path):
    assert classify_file(path, _EMPTY) == FileType.RAW_SQL


# ---------------------------------------------------------------------------
# Python files with SQL markers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "content",
    [
        _PYTHON_WITH_OP_EXECUTE,
        _PYTHON_WITH_CURSOR_EXECUTE,
        _PYTHON_WITH_TEXT,
        _PYTHON_WITH_SESSION_EXECUTE,
        _PYTHON_WITH_ORM_QUERY,
    ],
)
def test_python_with_sql_markers(content):
    assert classify_file("db/queries.py", content) == FileType.PYTHON_WITH_SQL


# ---------------------------------------------------------------------------
# Ignored files
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path",
    [
        "README.md",
        "docs/index.rst",
        "setup.cfg",
        "pyproject.toml",
        "db/queries.py",  # plain Python, no markers
    ],
)
def test_ignored_files(path):
    assert classify_file(path, _PLAIN_PYTHON) == FileType.IGNORE


def test_readme_is_ignored():
    assert classify_file("README.md", "# Hello world") == FileType.IGNORE


# ---------------------------------------------------------------------------
# config.ignore_paths is respected
# ---------------------------------------------------------------------------


def test_ignore_paths_pattern_overrides_sql():
    """A .sql file matching an ignore_paths glob must be classified as IGNORE."""
    with patch("pgreviewer.parsing.file_classifier.settings") as mock_settings:
        mock_settings.IGNORE_PATHS = ["vendor/**"]
        mock_settings.TRIGGER_PATHS = []
        result = classify_file("vendor/third_party/init.sql", _EMPTY)
    assert result == FileType.IGNORE


def test_classify_file_supports_injected_ignore_and_trigger_paths():
    assert (
        classify_file(
            "vendor/seed.sql",
            _EMPTY,
            ignore_paths=["vendor/**"],
            trigger_paths=["**.sql"],
        )
        == FileType.IGNORE
    )
    assert (
        classify_file(
            "db/queries.sql",
            _EMPTY,
            ignore_paths=["vendor/**"],
            trigger_paths=["**.sql"],
        )
        == FileType.RAW_SQL
    )


def test_ignore_paths_pattern_overrides_migration():
    """A migration file matching an ignore_paths glob must be classified as IGNORE."""
    with patch("pgreviewer.parsing.file_classifier.settings") as mock_settings:
        mock_settings.IGNORE_PATHS = ["tests/fixtures/**"]
        mock_settings.TRIGGER_PATHS = []
        result = classify_file("tests/fixtures/migrations/seed.sql", _EMPTY)
    assert result == FileType.IGNORE


def test_ignore_paths_does_not_affect_unmatched_files():
    """Files that don't match any ignore pattern are classified normally."""
    with patch("pgreviewer.parsing.file_classifier.settings") as mock_settings:
        mock_settings.IGNORE_PATHS = ["docs/*"]
        mock_settings.TRIGGER_PATHS = []
        result = classify_file("db/queries.sql", _EMPTY)
    assert result == FileType.RAW_SQL


def test_trigger_paths_only_allow_matching_paths():
    with patch("pgreviewer.parsing.file_classifier.settings") as mock_settings:
        mock_settings.IGNORE_PATHS = []
        mock_settings.TRIGGER_PATHS = ["custom_sql/**"]
        assert classify_file("custom_sql/report.sql", _EMPTY) == FileType.RAW_SQL
        assert classify_file("migrations/0001_init.sql", _EMPTY) == FileType.IGNORE


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_content_non_python():
    assert classify_file("config.yaml", _EMPTY) == FileType.IGNORE


def test_migration_python_without_sql_content_is_still_migration_python():
    """Classification is path-based for migration dirs, content does not matter."""
    assert (
        classify_file("alembic/versions/001_no_sql.py", _PLAIN_PYTHON)
        == FileType.MIGRATION_PYTHON
    )


def test_sql_file_in_migration_dir_is_migration_sql_not_raw():
    assert classify_file("migrations/init.sql", _EMPTY) == FileType.MIGRATION_SQL


def test_python_file_without_sql_markers_is_ignored():
    assert classify_file("app/models.py", _PLAIN_PYTHON) == FileType.IGNORE


# ---------------------------------------------------------------------------
# SQLAlchemy declarative model files
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "content",
    [
        _SQLALCHEMY_MODEL,
        _DECLARATIVE_BASE,
        _DECLARATIVE_BASE_V2,
    ],
)
def test_sqlalchemy_model_file_classified_as_python_with_sql(content):
    """Pure ORM model files (no .execute calls) must not be ignored."""
    assert classify_file("app/models.py", content) == FileType.PYTHON_WITH_SQL


def test_sqlalchemy_model_in_migration_dir_stays_migration_python():
    """Migration-dir path takes priority over content-based SQLAlchemy detection."""
    assert (
        classify_file("alembic/versions/001_models.py", _SQLALCHEMY_MODEL)
        == FileType.MIGRATION_PYTHON
    )


# ---------------------------------------------------------------------------
# SQLAlchemy ORM query patterns in router / service files
# ---------------------------------------------------------------------------


def test_orm_query_in_router_classified_as_python_with_sql():
    """Router files using db.query() must be classified so N+1 detectors fire."""
    assert (
        classify_file("app/routers/standup.py", _PYTHON_WITH_ORM_QUERY)
        == FileType.PYTHON_WITH_SQL
    )


def test_orm_query_in_migration_dir_stays_migration_python():
    assert (
        classify_file("alembic/versions/001_router.py", _PYTHON_WITH_ORM_QUERY)
        == FileType.MIGRATION_PYTHON
    )
