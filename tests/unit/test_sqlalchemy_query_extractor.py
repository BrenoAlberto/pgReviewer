"""Unit tests for pgreviewer.parsing.sqlalchemy_query_extractor.

Covers:
- extract_orm_queries: session.query().filter() pattern → ExtractedQuery
- extract_orm_queries: select().where() pattern → ExtractedQuery
- extract_orm_queries: order_by chain → ORDER BY clause
- extract_orm_queries: join chain → JOIN clause
- extract_orm_queries: filter_by keyword style → WHERE clause
- extract_orm_queries: multi-step chain (filter + order_by)
- extract_orm_queries: metadata (extraction_method, confidence, notes)
- extract_orm_queries: fixture file with all patterns
- Edge cases: empty source, no ORM queries, whitespace-only source

No database connection is required.
"""

from __future__ import annotations

from pathlib import Path

from pgreviewer.core.models import ExtractedQuery
from pgreviewer.parsing.sqlalchemy_query_extractor import extract_orm_queries

# ---------------------------------------------------------------------------
# Fixture path
# ---------------------------------------------------------------------------

_FIXTURE = (
    Path(__file__).parent.parent
    / "fixtures"
    / "python_sources"
    / "sqlalchemy_orm_queries.py"
)


# ===========================================================================
# Core requirement from the issue: session.query().filter() → approximate SQL
# ===========================================================================


class TestSessionQueryFilter:
    """session.query(Order).filter(Order.user_id == user_id) → SELECT … WHERE …"""

    SOURCE = """\
def f(session, user_id):
    return session.query(Order).filter(Order.user_id == user_id).all()
"""

    def setup_method(self):
        self.results = extract_orm_queries(self.SOURCE)

    def test_returns_one_result(self):
        assert len(self.results) == 1

    def test_sql_select_from_orders(self):
        assert "SELECT * FROM orders" in self.results[0].sql

    def test_sql_where_user_id(self):
        assert "WHERE user_id = :user_id" in self.results[0].sql

    def test_extraction_method(self):
        assert self.results[0].extraction_method == "treesitter_orm"

    def test_confidence(self):
        assert self.results[0].confidence == 0.7

    def test_notes_orm_message(self):
        assert "ORM query" in self.results[0].notes

    def test_result_is_extracted_query(self):
        assert isinstance(self.results[0], ExtractedQuery)

    def test_line_number_positive(self):
        assert self.results[0].line_number > 0


# ===========================================================================
# select().where() – SQLAlchemy Core style
# ===========================================================================


class TestSelectWhere:
    """select(User).where(User.id == user_id) → SELECT * FROM users WHERE id = :user_id.

    Tests the SQLAlchemy Core select() pattern.
    """

    SOURCE = """\
def f(session, user_id):
    return session.execute(select(User).where(User.id == user_id)).scalars()
"""

    def setup_method(self):
        self.results = extract_orm_queries(self.SOURCE)

    def test_returns_one_result(self):
        assert len(self.results) == 1

    def test_sql_select_from_users(self):
        assert "SELECT * FROM users" in self.results[0].sql

    def test_sql_where_id(self):
        assert "WHERE id = :user_id" in self.results[0].sql

    def test_extraction_method(self):
        assert self.results[0].extraction_method == "treesitter_orm"

    def test_confidence(self):
        assert self.results[0].confidence == 0.7


# ===========================================================================
# order_by chain
# ===========================================================================


class TestOrderBy:
    """session.query(Order).order_by(Order.created_at) → … ORDER BY created_at"""

    SOURCE = """\
def f(session):
    return session.query(Order).order_by(Order.created_at).all()
"""

    def setup_method(self):
        self.results = extract_orm_queries(self.SOURCE)

    def test_returns_one_result(self):
        assert len(self.results) == 1

    def test_sql_contains_order_by(self):
        assert "ORDER BY created_at" in self.results[0].sql

    def test_sql_select_from_orders(self):
        assert "SELECT * FROM orders" in self.results[0].sql


# ===========================================================================
# join chain
# ===========================================================================


class TestJoin:
    """session.query(Order).join(Item) → … JOIN items"""

    SOURCE = """\
def f(session):
    return session.query(Order).join(Item).all()
"""

    def setup_method(self):
        self.results = extract_orm_queries(self.SOURCE)

    def test_returns_one_result(self):
        assert len(self.results) == 1

    def test_sql_contains_join(self):
        assert "JOIN items" in self.results[0].sql

    def test_sql_select_from_orders(self):
        assert "SELECT * FROM orders" in self.results[0].sql


# ===========================================================================
# filter_by keyword style
# ===========================================================================


class TestFilterBy:
    """session.query(User).filter_by(email=email) → WHERE email = :email"""

    SOURCE = """\
def f(session, email):
    return session.query(User).filter_by(email=email).first()
"""

    def setup_method(self):
        self.results = extract_orm_queries(self.SOURCE)

    def test_returns_one_result(self):
        assert len(self.results) == 1

    def test_sql_where_email(self):
        assert "WHERE email = :email" in self.results[0].sql

    def test_sql_select_from_users(self):
        assert "SELECT * FROM users" in self.results[0].sql


# ===========================================================================
# Multi-step chain: filter + order_by
# ===========================================================================


class TestMultiStepChain:
    """session.query(Order).filter(...).order_by(...) → WHERE … ORDER BY …"""

    SOURCE = """\
def f(session, user_id):
    return (
        session.query(Order)
        .filter(Order.user_id == user_id)
        .order_by(Order.created_at)
        .all()
    )
"""

    def setup_method(self):
        self.results = extract_orm_queries(self.SOURCE)

    def test_returns_one_result(self):
        assert len(self.results) == 1

    def test_sql_has_where(self):
        assert "WHERE user_id = :user_id" in self.results[0].sql

    def test_sql_has_order_by(self):
        assert "ORDER BY created_at" in self.results[0].sql

    def test_sql_select_from_orders(self):
        assert "SELECT * FROM orders" in self.results[0].sql


# ===========================================================================
# Multiple queries in one source file
# ===========================================================================


class TestMultipleQueries:
    """Multiple ORM calls in one source → one result per call."""

    SOURCE = """\
def get_orders(session, user_id):
    return session.query(Order).filter(Order.user_id == user_id).all()

def get_users(session):
    return session.execute(select(User)).scalars()
"""

    def setup_method(self):
        self.results = extract_orm_queries(self.SOURCE)

    def test_returns_two_results(self):
        assert len(self.results) == 2

    def test_results_sorted_by_line(self):
        lines = [r.line_number for r in self.results]
        assert lines == sorted(lines)

    def test_first_is_orders(self):
        assert "orders" in self.results[0].sql

    def test_second_is_users(self):
        assert "users" in self.results[1].sql


# ===========================================================================
# Fixture file – all six patterns
# ===========================================================================


class TestFixtureFile:
    """High-level smoke test against the six-pattern fixture file."""

    def setup_method(self):
        source = _FIXTURE.read_text()
        self.results = extract_orm_queries(source, str(_FIXTURE))

    def test_finds_at_least_six_queries(self):
        # fixture has 6 ORM patterns
        assert len(self.results) >= 6

    def test_all_are_extracted_query(self):
        assert all(isinstance(r, ExtractedQuery) for r in self.results)

    def test_all_have_orm_method(self):
        assert all(r.extraction_method == "treesitter_orm" for r in self.results)

    def test_all_have_07_confidence(self):
        assert all(r.confidence == 0.7 for r in self.results)

    def test_all_have_notes(self):
        assert all(r.notes is not None for r in self.results)

    def test_source_file_set(self):
        assert all(r.source_file == str(_FIXTURE) for r in self.results)

    def test_sql_strings_not_empty(self):
        assert all(r.sql.strip() for r in self.results)

    def test_sorted_by_line_number(self):
        lines = [r.line_number for r in self.results]
        assert lines == sorted(lines)


# ===========================================================================
# Edge cases
# ===========================================================================


class TestEmptySource:
    def test_empty_string(self):
        assert extract_orm_queries("") == []

    def test_whitespace_only(self):
        assert extract_orm_queries("   \n\t  ") == []


class TestNoOrmQueries:
    def test_plain_python_file(self):
        source = """\
def hello():
    return "world"

x = 1 + 2
"""
        assert extract_orm_queries(source) == []

    def test_raw_sql_execute(self):
        source = """\
cursor.execute("SELECT * FROM users WHERE id = %s", [1])
"""
        assert extract_orm_queries(source) == []


# ===========================================================================
# to_dict / from_dict round-trip on ORM results
# ===========================================================================


class TestExtractedQuerySerialization:
    SOURCE = """\
session.query(Order).filter(Order.user_id == user_id)
"""

    def setup_method(self):
        self.result = extract_orm_queries(self.SOURCE)[0]

    def test_round_trip(self):
        d = self.result.to_dict()
        restored = ExtractedQuery.from_dict(d)
        assert restored.sql == self.result.sql
        assert restored.extraction_method == "treesitter_orm"
        assert restored.confidence == 0.7
        assert restored.notes == self.result.notes
