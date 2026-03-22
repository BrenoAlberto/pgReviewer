"""Unit tests for the parsing layer.

Covers:
- diff_parser: file/line extraction from fixture patch files
- file_classifier: path + content classification
- sql_extractor_migration: Alembic and raw-SQL fixture files → ExtractedQuery
- sql_extractor_raw: tree-sitter extraction from Python source fixtures
- TSParser: tree-sitter infrastructure
- param_substitutor: $1, %s, :param placeholder substitution

No database connection is required.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
import tree_sitter_python as tspython
from tree_sitter import Language, Parser, Query, QueryCursor

from pgreviewer.core.models import ExtractedQuery
from pgreviewer.parsing.diff_parser import ChangedFile, parse_diff
from pgreviewer.parsing.file_classifier import FileType, classify_file
from pgreviewer.parsing.param_substitutor import make_notes, substitute_params
from pgreviewer.parsing.sql_extractor_migration import (
    extract_from_alembic_file,
    extract_from_sql_file,
)
from pgreviewer.parsing.sql_extractor_raw import extract_raw_sql

# ---------------------------------------------------------------------------
# Fixture directories
# ---------------------------------------------------------------------------

_DIFFS_DIR = Path(__file__).parent.parent / "fixtures" / "diffs"
_MIGRATIONS_DIR = Path(__file__).parent.parent / "fixtures" / "migrations"
_PYTHON_SOURCES_DIR = Path(__file__).parent.parent / "fixtures" / "python_sources"


def _load_diff(name: str) -> str:
    return (_DIFFS_DIR / name).read_text()


def _python_source(name: str) -> str:
    return (_PYTHON_SOURCES_DIR / name).read_text()


# ===========================================================================
# diff_parser
# ===========================================================================


class TestDiffParserAlembicPatch:
    """Tests against alembic_add_column.patch."""

    def setup_method(self):
        self.result = parse_diff(_load_diff("alembic_add_column.patch"))

    def test_returns_one_file(self):
        assert len(self.result) == 1

    def test_correct_file_path(self):
        assert self.result[0].path == "alembic/versions/0005_add_email_column.py"

    def test_is_new_file(self):
        assert self.result[0].is_new_file is True

    def test_added_lines_count(self):
        assert len(self.result[0].added_lines) == 16

    def test_line_numbers_start_at_one(self):
        assert self.result[0].added_line_numbers[0] == 1

    def test_op_execute_line_present(self):
        lines = self.result[0].added_lines
        assert any("op.execute" in line for line in lines)

    def test_all_types_correct(self):
        assert all(isinstance(f, ChangedFile) for f in self.result)


class TestDiffParserRawSqlPatch:
    """Tests against raw_sql_migration.patch."""

    def setup_method(self):
        self.result = parse_diff(_load_diff("raw_sql_migration.patch"))

    def test_returns_one_file(self):
        assert len(self.result) == 1

    def test_correct_file_path(self):
        assert self.result[0].path == "migrations/0003_add_products.sql"

    def test_is_new_file(self):
        assert self.result[0].is_new_file is True

    def test_create_table_line_present(self):
        lines = self.result[0].added_lines
        assert any("CREATE TABLE" in line for line in lines)

    def test_create_index_line_present(self):
        lines = self.result[0].added_lines
        assert any("CREATE INDEX" in line for line in lines)

    def test_line_numbers_populated(self):
        f = self.result[0]
        assert len(f.added_lines) == len(f.added_line_numbers)
        assert all(isinstance(n, int) and n >= 1 for n in f.added_line_numbers)


class TestDiffParserPythonExecutePatch:
    """Tests against python_execute.patch."""

    def setup_method(self):
        self.result = parse_diff(_load_diff("python_execute.patch"))

    def test_returns_one_file(self):
        assert len(self.result) == 1

    def test_correct_file_path(self):
        assert self.result[0].path == "db/user_queries.py"

    def test_is_new_file(self):
        assert self.result[0].is_new_file is True

    def test_cursor_execute_line_present(self):
        lines = self.result[0].added_lines
        assert any("cursor.execute" in line for line in lines)

    def test_added_lines_count(self):
        assert len(self.result[0].added_lines) == 10

    def test_no_leading_plus_on_lines(self):
        for line in self.result[0].added_lines:
            assert not line.startswith("+")


class TestDiffParserNoSqlChangesPatch:
    """Tests against no_sql_changes.patch (README + pyproject only)."""

    def setup_method(self):
        self.result = parse_diff(_load_diff("no_sql_changes.patch"))

    def test_returns_two_files(self):
        assert len(self.result) == 2

    def test_paths_are_non_sql(self):
        paths = [f.path for f in self.result]
        assert "README.md" in paths
        assert "pyproject.toml" in paths

    def test_no_file_is_new(self):
        for f in self.result:
            assert f.is_new_file is False

    def test_no_sql_content_in_added_lines(self):
        for f in self.result:
            for line in f.added_lines:
                assert "SELECT" not in line.upper()
                assert "CREATE TABLE" not in line.upper()


# ===========================================================================
# file_classifier
# ===========================================================================


@pytest.mark.parametrize(
    "path,content,expected",
    [
        # Alembic migration Python file
        (
            "alembic/versions/0005_add_email.py",
            "",
            FileType.MIGRATION_PYTHON,
        ),
        # Regular migrations dir Python file
        (
            "migrations/0001_init.py",
            "",
            FileType.MIGRATION_PYTHON,
        ),
        # Migration SQL file
        (
            "migrations/0003_add_products.sql",
            "",
            FileType.MIGRATION_SQL,
        ),
        # Raw SQL outside migrations
        (
            "db/queries.sql",
            "",
            FileType.RAW_SQL,
        ),
        # Python file with cursor.execute
        (
            "db/user_queries.py",
            'cursor.execute("SELECT 1")',
            FileType.PYTHON_WITH_SQL,
        ),
        # Python file with op.execute
        (
            "app/migrate.py",
            'op.execute("ALTER TABLE ...")',
            FileType.PYTHON_WITH_SQL,
        ),
        # Plain Python without SQL markers
        (
            "app/utils.py",
            "def add(a, b):\n    return a + b\n",
            FileType.IGNORE,
        ),
        # README is always ignored
        (
            "README.md",
            "# Hello world",
            FileType.IGNORE,
        ),
        # pyproject.toml is always ignored
        (
            "pyproject.toml",
            "[project]\nname = 'myapp'",
            FileType.IGNORE,
        ),
    ],
)
def test_classify_file_parametrized(path, content, expected):
    assert classify_file(path, content) == expected


def test_classify_alembic_patch_file_path():
    """The file from alembic_add_column.patch is a MIGRATION_PYTHON."""
    path = "alembic/versions/0005_add_email_column.py"
    assert classify_file(path, "") == FileType.MIGRATION_PYTHON


def test_classify_raw_sql_patch_file_path():
    """The file from raw_sql_migration.patch is a MIGRATION_SQL."""
    path = "migrations/0003_add_products.sql"
    assert classify_file(path, "") == FileType.MIGRATION_SQL


def test_classify_python_execute_patch_file_path():
    """The db/user_queries.py from python_execute.patch is PYTHON_WITH_SQL."""
    path = "db/user_queries.py"
    content = _python_source("simple_execute.py")
    assert classify_file(path, content) == FileType.PYTHON_WITH_SQL


def test_classify_no_sql_changes_readme():
    """README.md from no_sql_changes.patch is IGNORE."""
    assert classify_file("README.md", "## Installation") == FileType.IGNORE


def test_classify_ignore_paths_overrides_sql():
    with patch("pgreviewer.parsing.file_classifier.settings") as mock_settings:
        mock_settings.IGNORE_PATHS = ["vendor/**"]
        assert classify_file("vendor/init.sql", "") == FileType.IGNORE


# ===========================================================================
# sql_extractor_migration — fixture files in, ExtractedQuery objects out
# ===========================================================================


class TestExtractFromAlembicFixture:
    """Tests against tests/fixtures/migrations/add_column_migration.py."""

    def setup_method(self):
        self.queries = extract_from_alembic_file(
            _MIGRATIONS_DIR / "add_column_migration.py"
        )

    def test_returns_list_of_extracted_queries(self):
        assert isinstance(self.queries, list)
        assert all(isinstance(q, ExtractedQuery) for q in self.queries)

    def test_upgrade_queries_only(self):
        # downgrade() is excluded — op.add_column + 2 op.execute calls in upgrade()
        assert len(self.queries) == 3

    def test_create_index_extracted(self):
        sqls = [q.sql for q in self.queries]
        assert any("CREATE INDEX" in s for s in sqls)

    def test_update_extracted(self):
        sqls = [q.sql for q in self.queries]
        assert any("UPDATE" in s for s in sqls)

    def test_extraction_method_is_migration_sql(self):
        for q in self.queries:
            assert q.extraction_method in ("migration_sql", "alembic_op")

    def test_confidence_is_one(self):
        for q in self.queries:
            assert q.confidence == 1.0

    def test_source_file_set(self):
        for q in self.queries:
            assert "add_column_migration.py" in q.source_file

    def test_line_numbers_positive(self):
        for q in self.queries:
            assert q.line_number >= 1


class TestExtractFromSqlFixture:
    """Tests against tests/fixtures/migrations/add_products.sql."""

    def setup_method(self):
        self.queries = extract_from_sql_file(_MIGRATIONS_DIR / "add_products.sql")

    def test_returns_two_statements(self):
        assert len(self.queries) == 2

    def test_create_table_first(self):
        assert "CREATE TABLE" in self.queries[0].sql

    def test_create_index_second(self):
        assert "CREATE INDEX" in self.queries[1].sql

    def test_extraction_method(self):
        for q in self.queries:
            assert q.extraction_method == "migration_sql"

    def test_line_numbers_ascending(self):
        assert self.queries[0].line_number < self.queries[1].line_number


def test_extract_from_alembic_no_execute(tmp_path):
    """An Alembic file with no op.execute calls returns an empty list."""
    migration = tmp_path / "empty_migration.py"
    migration.write_text(
        "from alembic import op\n\ndef upgrade():\n    op.add_column('t', None)\n"
    )
    assert extract_from_alembic_file(migration) == []


def test_extract_from_sql_file_empty(tmp_path):
    """A SQL file with only comments returns an empty list."""
    sql_file = tmp_path / "comments_only.sql"
    sql_file.write_text("-- This is just a comment\n-- Another comment\n")
    assert extract_from_sql_file(sql_file) == []


# ===========================================================================
# sql_extractor_raw — tree-sitter based, Python source fixtures
# ===========================================================================


class TestExtractRawSimpleExecute:
    """Tests against fixtures/python_sources/simple_execute.py."""

    def setup_method(self):
        src = _python_source("simple_execute.py")
        self.queries = extract_raw_sql(src, file_path="simple_execute.py")

    def test_one_query_found(self):
        assert len(self.queries) == 1

    def test_sql_content(self):
        assert "SELECT" in self.queries[0].sql
        assert "users" in self.queries[0].sql

    def test_high_confidence(self):
        assert self.queries[0].confidence == 0.9

    def test_extraction_method(self):
        assert self.queries[0].extraction_method == "treesitter"

    def test_line_number_positive(self):
        assert self.queries[0].line_number >= 1


class TestExtractRawSqlalchemyText:
    """Tests against fixtures/python_sources/sqlalchemy_text.py."""

    def setup_method(self):
        src = _python_source("sqlalchemy_text.py")
        self.queries = extract_raw_sql(src, file_path="sqlalchemy_text.py")

    def test_one_query_found(self):
        assert len(self.queries) == 1

    def test_sql_content(self):
        assert "SELECT" in self.queries[0].sql
        assert "orders" in self.queries[0].sql

    def test_high_confidence(self):
        assert self.queries[0].confidence == 0.9

    def test_text_wrapper_unwrapped(self):
        """text() wrapper should be stripped; raw SQL should be the result."""
        assert not self.queries[0].sql.startswith("text(")


class TestExtractRawAsyncpgFetch:
    """Tests against fixtures/python_sources/asyncpg_fetch.py."""

    def setup_method(self):
        src = _python_source("asyncpg_fetch.py")
        self.queries = extract_raw_sql(src, file_path="asyncpg_fetch.py")

    def test_two_queries_found(self):
        assert len(self.queries) == 2

    def test_fetchrow_query(self):
        sqls = [q.sql for q in self.queries]
        assert any("users" in s for s in sqls)

    def test_fetch_query(self):
        sqls = [q.sql for q in self.queries]
        assert any("products" in s for s in sqls)

    def test_all_high_confidence(self):
        for q in self.queries:
            assert q.confidence == 0.9


class TestExtractRawFstringDynamic:
    """f-string queries must be flagged as low confidence."""

    def setup_method(self):
        src = _python_source("fstring_dynamic.py")
        self.queries = extract_raw_sql(src, file_path="fstring_dynamic.py")

    def test_one_query_found(self):
        assert len(self.queries) == 1

    def test_low_confidence(self):
        assert self.queries[0].confidence < 0.5

    def test_confidence_is_0_3(self):
        assert self.queries[0].confidence == 0.3


class TestExtractRawNoSql:
    """A pure-Python file with no SQL patterns returns no results."""

    def test_empty_results(self):
        src = _python_source("no_sql.py")
        queries = extract_raw_sql(src, file_path="no_sql.py")
        assert queries == []

    def test_empty_string_returns_empty(self):
        assert extract_raw_sql("") == []


class TestExtractRawConcatenatedSql:
    """String concatenation must be flagged as low confidence."""

    def setup_method(self):
        src = _python_source("concatenated_sql.py")
        self.queries = extract_raw_sql(src, file_path="concatenated_sql.py")

    def test_one_query_found(self):
        assert len(self.queries) == 1

    def test_low_confidence(self):
        assert self.queries[0].confidence < 0.5


# ===========================================================================
# TSParser — tree-sitter infrastructure
# ===========================================================================


class TestTSParser:
    """Verify the tree-sitter Python parser and query execution work end-to-end."""

    def setup_method(self):
        self.language = Language(tspython.language())
        self.parser = Parser(self.language)

    def test_parse_returns_tree(self):
        source = b"x = 1\n"
        tree = self.parser.parse(source)
        assert tree is not None
        assert tree.root_node is not None

    def test_root_node_type_is_module(self):
        source = b"x = 1\n"
        tree = self.parser.parse(source)
        assert tree.root_node.type == "module"

    def test_execute_call_captured(self):
        """A tree-sitter query targeting execute() calls should capture string args."""
        source = b'cursor.execute("SELECT * FROM users")\n'
        tree = self.parser.parse(source)

        query = Query(
            self.language,
            """
            (call
              function: (attribute
                attribute: (identifier) @method)
              arguments: (argument_list
                (string) @sql)
              (#eq? @method "execute"))
            """,
        )
        cursor = QueryCursor(query)
        captures = cursor.captures(tree.root_node)

        assert captures.get("method")
        assert captures.get("sql")
        assert captures["method"][0].text == b"execute"
        assert b"SELECT * FROM users" in captures["sql"][0].text

    def test_no_captures_on_non_matching_code(self):
        source = b"print('hello world')\n"
        tree = self.parser.parse(source)

        query = Query(
            self.language,
            """
            (call
              function: (attribute
                attribute: (identifier) @method)
              arguments: (argument_list (string) @sql)
              (#eq? @method "execute"))
            """,
        )
        cursor = QueryCursor(query)
        captures = cursor.captures(tree.root_node)

        assert not captures.get("method")
        assert not captures.get("sql")

    def test_simple_execute_fixture_has_execute_captures(self):
        """Run tree-sitter query against simple_execute.py fixture."""
        source = _python_source("simple_execute.py").encode("utf-8")
        tree = self.parser.parse(source)

        query = Query(
            self.language,
            """
            (call
              function: (attribute
                attribute: (identifier) @method)
              arguments: (argument_list) @args
              (#match? @method "^(execute|fetch|fetchrow)$"))
            """,
        )
        cursor = QueryCursor(query)
        captures = cursor.captures(tree.root_node)

        assert captures.get("method")
        method_names = [n.text for n in captures["method"]]
        assert b"execute" in method_names

    def test_asyncpg_fixture_fetchrow_captured(self):
        """fetchrow and fetch calls are captured in asyncpg_fetch.py fixture."""
        source = _python_source("asyncpg_fetch.py").encode("utf-8")
        tree = self.parser.parse(source)

        query = Query(
            self.language,
            """
            (call
              function: (attribute
                attribute: (identifier) @method)
              arguments: (argument_list) @args
              (#match? @method "^(fetch|fetchrow)$"))
            """,
        )
        cursor = QueryCursor(query)
        captures = cursor.captures(tree.root_node)

        method_names = {n.text for n in captures.get("method", [])}
        assert b"fetchrow" in method_names
        assert b"fetch" in method_names

    def test_fstring_interpolation_node_present(self):
        """f-strings contain interpolation child nodes."""
        source = b'cursor.execute(f"SELECT * FROM {table}")\n'
        tree = self.parser.parse(source)

        # Walk to find the f-string node
        def _find_fstring(node):
            if node.type == "string":
                return node
            for child in node.children:
                result = _find_fstring(child)
                if result:
                    return result
            return None

        fstring_node = _find_fstring(tree.root_node)
        assert fstring_node is not None
        has_interpolation = any(
            c.type == "interpolation" for c in fstring_node.children
        )
        assert has_interpolation


# ===========================================================================
# param_substitutor
# ===========================================================================


class TestParamSubstitutorPgPositional:
    """$1, $2, … (PostgreSQL positional) substitution."""

    def test_single_placeholder_replaced(self):
        sql = "SELECT * FROM users WHERE id = $1"
        result, subs = substitute_params(sql)
        assert "$1" not in result
        assert len(subs) == 1

    def test_id_column_gets_integer_dummy(self):
        sql = "SELECT * FROM orders WHERE user_id = $1"
        result, subs = substitute_params(sql)
        assert result == "SELECT * FROM orders WHERE user_id = 42"
        assert "$1=42" in subs[0]

    def test_date_column_gets_now(self):
        sql = "SELECT * FROM events WHERE created_at = $1"
        result, subs = substitute_params(sql)
        assert "NOW()" in result

    def test_no_context_uses_position_cycle(self):
        sql = "SELECT $1, $2, $3"
        result, subs = substitute_params(sql)
        assert result == "SELECT 42, 'placeholder', NOW()"
        assert len(subs) == 3


class TestParamSubstitutorPsycopg:
    """%s (psycopg2 positional) substitution."""

    def test_single_placeholder_replaced(self):
        sql = "SELECT * FROM users WHERE id = %s"
        result, subs = substitute_params(sql)
        assert "%s" not in result
        assert len(subs) == 1

    def test_user_id_column_gets_integer(self):
        sql = "SELECT * FROM orders WHERE user_id = %s"
        result, subs = substitute_params(sql)
        assert result == "SELECT * FROM orders WHERE user_id = 42"
        assert "%s[1]=42" in subs[0]

    def test_multiple_placeholders(self):
        sql = "SELECT * FROM orders WHERE user_id = %s AND status = %s"
        result, subs = substitute_params(sql)
        assert "%s" not in result
        assert len(subs) == 2

    def test_no_context_cycles(self):
        sql = "SELECT %s, %s"
        result, subs = substitute_params(sql)
        assert result == "SELECT 42, 'placeholder'"


class TestParamSubstitutorNamed:
    """:param_name (SQLAlchemy named) substitution."""

    def test_user_id_named(self):
        sql = "SELECT * FROM orders WHERE user_id = :user_id"
        result, subs = substitute_params(sql)
        assert ":user_id" not in result
        assert ":user_id=42" in subs[0]

    def test_created_at_named(self):
        sql = "SELECT * FROM logs WHERE created_at > :created_at"
        result, subs = substitute_params(sql)
        assert "NOW()" in result

    def test_multiple_named_params(self):
        sql = "SELECT * FROM orders WHERE user_id = :user_id AND status = :status"
        result, subs = substitute_params(sql)
        assert ":user_id" not in result
        assert ":status" not in result
        assert len(subs) == 2

    def test_cast_syntax_not_treated_as_param(self):
        """::cast notation must not be matched as a named parameter."""
        sql = "SELECT $1::text"
        result, subs = substitute_params(sql)
        assert "::text" in result
        assert ":text" not in [s.split("=")[0] for s in subs]


class TestParamSubstitutorNoOp:
    """SQL without parameters must be returned unchanged."""

    def test_no_params_returns_original(self):
        sql = "SELECT * FROM users WHERE id = 1"
        result, subs = substitute_params(sql)
        assert result == sql
        assert subs == []


class TestMakeNotes:
    def test_non_empty_substitutions(self):
        subs = ["$1=42 (column: user_id)", "$2='placeholder'"]
        notes = make_notes(subs)
        assert notes is not None
        assert "analyzed with dummy parameters:" in notes
        assert "$1=42" in notes

    def test_empty_substitutions_returns_none(self):
        assert make_notes([]) is None
