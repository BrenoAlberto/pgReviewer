"""Unit tests for pgreviewer.parsing.orm_compiler.

Covers:
- compile_orm_query: successful compilation of a Core select() expression
- compile_orm_query: successful compilation with an ORM model class
- compile_orm_query: WHERE clause rendered with literal value
- compile_orm_query: project_path with src/ added to sys.path
- compile_orm_query: returns None and logs INFO when model is missing
- compile_orm_query: returns None for an invalid/non-compilable expression
- compile_orm_query: returns None when expression evaluates to None
- compile_orm_query: does not raise on any failure
- compile_orm_query: COMPILED_CONFIDENCE and FALLBACK_CONFIDENCE constants

No database connection is required.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
from pathlib import Path

from sqlalchemy import Column, Integer, MetaData, String, Table, select

from pgreviewer.parsing.orm_compiler import (
    COMPILED_CONFIDENCE,
    FALLBACK_CONFIDENCE,
    compile_orm_query,
)

# ---------------------------------------------------------------------------
# Fixtures: minimal SQLAlchemy Core table and ORM model
# ---------------------------------------------------------------------------

_meta = MetaData()
_users_table = Table(
    "users",
    _meta,
    Column("id", Integer, primary_key=True),
    Column("name", String),
)


def _make_orm_user_class():
    """Return a fresh User ORM model (new Base each time to avoid mapper conflicts)."""
    from sqlalchemy.orm import DeclarativeBase

    class Base(DeclarativeBase):
        pass

    class User(Base):
        __tablename__ = "users_orm"
        id = Column(Integer, primary_key=True)
        name = Column(String)

    return User


# ===========================================================================
# Confidence constants
# ===========================================================================


class TestConfidenceConstants:
    def test_compiled_confidence(self):
        assert COMPILED_CONFIDENCE == 0.95

    def test_fallback_confidence(self):
        assert FALLBACK_CONFIDENCE == 0.7


# ===========================================================================
# Successful compilation — Core select()
# ===========================================================================


class TestCompileCoreSelect:
    """compile_orm_query succeeds for a plain Core select() expression."""

    def test_returns_string(self):
        sql = compile_orm_query(
            "select(users)",
            extra_namespace={"users": _users_table, "select": select},
        )
        assert isinstance(sql, str)

    def test_sql_contains_table_name(self):
        sql = compile_orm_query(
            "select(users)",
            extra_namespace={"users": _users_table, "select": select},
        )
        assert sql is not None
        assert "users" in sql.lower()

    def test_sql_contains_select(self):
        sql = compile_orm_query(
            "select(users)",
            extra_namespace={"users": _users_table, "select": select},
        )
        assert sql is not None
        assert sql.upper().startswith("SELECT")


# ===========================================================================
# Successful compilation — ORM model class
# ===========================================================================


class TestCompileOrmModel:
    """compile_orm_query succeeds when model classes are provided
    via extra_namespace."""

    def setup_method(self):
        self.User = _make_orm_user_class()

    def test_returns_string(self):
        sql = compile_orm_query(
            "select(User)",
            extra_namespace={"User": self.User, "select": select},
        )
        assert isinstance(sql, str)

    def test_sql_starts_with_select(self):
        sql = compile_orm_query(
            "select(User)",
            extra_namespace={"User": self.User, "select": select},
        )
        assert sql is not None
        assert sql.upper().startswith("SELECT")

    def test_sql_contains_table_name(self):
        sql = compile_orm_query(
            "select(User)",
            extra_namespace={"User": self.User, "select": select},
        )
        assert sql is not None
        assert "users_orm" in sql.lower()

    def test_where_clause_with_literal(self):
        """Literal value in WHERE is rendered with literal_binds=True."""
        sql = compile_orm_query(
            "select(User).where(User.id == 42)",
            extra_namespace={"User": self.User, "select": select},
        )
        assert sql is not None
        assert "42" in sql
        assert "WHERE" in sql.upper()


# ===========================================================================
# Failure cases — returns None, no crash
# ===========================================================================


class TestCompileFailureReturnNone:
    """compile_orm_query returns None (not raises) on any failure."""

    def test_undefined_model_returns_none(self):
        # 'NonExistentModel' is not in the namespace
        result = compile_orm_query("select(NonExistentModel)")
        assert result is None

    def test_invalid_expression_returns_none(self):
        result = compile_orm_query("this is not python")
        assert result is None

    def test_none_expression_returns_none(self):
        result = compile_orm_query("None")
        assert result is None

    def test_non_compilable_object_returns_none(self):
        # Returns a plain dict — not a SQLAlchemy compilable object
        result = compile_orm_query('{"key": "value"}')
        assert result is None

    def test_non_compilable_string_returns_none(self):
        result = compile_orm_query('"just a string"')
        assert result is None


# ===========================================================================
# Failure is logged at INFO level
# ===========================================================================


class TestFailureLogging:
    """Failures are logged at INFO (not ERROR/WARNING) so as not to alarm callers."""

    def test_logs_info_on_undefined_model(self, caplog):
        with caplog.at_level(logging.INFO, logger="pgreviewer.parsing.orm_compiler"):
            compile_orm_query("select(MissingModel)")
        assert any(r.levelno == logging.INFO for r in caplog.records)

    def test_logs_info_on_invalid_expression(self, caplog):
        with caplog.at_level(logging.INFO, logger="pgreviewer.parsing.orm_compiler"):
            compile_orm_query("!!!invalid python!!!")
        assert any(r.levelno == logging.INFO for r in caplog.records)

    def test_does_not_log_error_on_failure(self, caplog):
        with caplog.at_level(logging.WARNING, logger="pgreviewer.parsing.orm_compiler"):
            compile_orm_query("select(MissingModel)")
        # No WARNING or ERROR records should be emitted
        assert not any(r.levelno >= logging.WARNING for r in caplog.records)


# ===========================================================================
# project_path: sys.path mutation is temporary
# ===========================================================================


class TestProjectPathSysPath:
    """project_path/src is added to sys.path only for the duration of the call."""

    def test_src_not_left_in_sys_path(self, tmp_path):
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        src_str = str(src_dir.resolve())

        assert src_str not in sys.path
        # Compilation will fail (no models), but sys.path should be restored
        compile_orm_query("select(SomeModel)", project_path=tmp_path)
        assert src_str not in sys.path

    def test_project_root_not_left_in_sys_path(self, tmp_path):
        # tmp_path itself (no src/ subdir)
        project_str = str(tmp_path.resolve())
        before = list(sys.path)
        compile_orm_query("select(SomeModel)", project_path=tmp_path)
        assert sys.path == before or project_str not in sys.path

    def test_importable_module_via_project_path(self, tmp_path):
        """When the project has an importable module, extra_namespace can use it."""
        # Write a tiny module to tmp_path/src/
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        module_file = src_dir / "mymodels.py"
        module_file.write_text(
            "from sqlalchemy import Column, Integer\n"
            "from sqlalchemy.orm import DeclarativeBase\n"
            "class Base(DeclarativeBase): pass\n"
            "class Widget(Base):\n"
            "    __tablename__ = 'widgets'\n"
            "    id = Column(Integer, primary_key=True)\n"
        )

        # Import the module while project_path is on sys.path
        src_str = str(src_dir.resolve())
        sys.path.insert(0, src_str)
        try:
            import mymodels  # type: ignore[import]

            widget_cls = mymodels.Widget
        finally:
            sys.path.remove(src_str)
            # Clean up cached import
            sys.modules.pop("mymodels", None)

        sql = compile_orm_query(
            "select(Widget)",
            extra_namespace={"Widget": widget_cls, "select": select},
        )
        assert sql is not None
        assert "widgets" in sql.lower()


# ===========================================================================
# Fixture project: sqlalchemy_models.py (User, Order, Item)
# ===========================================================================


class TestFixtureModels:
    """Compile queries using models from the test fixture file."""

    _FIXTURE = (
        Path(__file__).parent
        / "fixtures"
        / "python_sources"
        / "sqlalchemy_models.py"
    )

    def setup_method(self):
        # Load the fixture module by path so model classes are importable
        # without modifying sys.path or relying on package structure.
        spec = importlib.util.spec_from_file_location("_fixture_models", self._FIXTURE)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        self.User = mod.User
        self.Order = mod.Order
        self.Item = mod.Item

    def test_select_user(self):
        sql = compile_orm_query(
            "select(User)",
            extra_namespace={"User": self.User, "select": select},
        )
        assert sql is not None
        assert sql.upper().startswith("SELECT")
        assert "users" in sql.lower()

    def test_select_order_with_where(self):
        sql = compile_orm_query(
            "select(Order).where(Order.status == 'active')",
            extra_namespace={"Order": self.Order, "select": select},
        )
        assert sql is not None
        assert "orders" in sql.lower()
        assert "WHERE" in sql.upper()

    def test_select_item_literal(self):
        sql = compile_orm_query(
            "select(Item).where(Item.price > 100)",
            extra_namespace={"Item": self.Item, "select": select},
        )
        assert sql is not None
        assert "items" in sql.lower()
        assert "100" in sql
