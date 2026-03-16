"""Unit tests for pgreviewer.parsing.sqlalchemy_analyzer.

Covers:
- analyze_model_file / analyze_model_source on the fixture file
- Single-class and multi-class model files
- Column types, nullable, index, unique, primary_key flags
- ForeignKey target extraction
- Explicit Index definitions (standalone and in __table_args__)
- relationship back_populates and foreign_keys extraction
- Edge cases: no-model file, class without __tablename__, mixed sources

No database connection is required.
"""

from __future__ import annotations

from pathlib import Path

from pgreviewer.parsing.sqlalchemy_analyzer import (
    ColumnDef,
    FKDef,
    IndexDef,
    ModelDefinition,
    RelDef,
    analyze_model_file,
    analyze_model_source,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_FIXTURE = (
    Path(__file__).parent.parent
    / "fixtures"
    / "python_sources"
    / "sqlalchemy_models.py"
)


# ===========================================================================
# analyze_model_file – fixture file with three model classes
# ===========================================================================


class TestAnalyzeModelFile:
    """High-level tests against the three-class fixture file."""

    def setup_method(self):
        self.models: list[ModelDefinition] = analyze_model_file(_FIXTURE)

    def test_returns_three_models(self):
        assert len(self.models) == 3

    def test_model_names(self):
        names = [m.class_name for m in self.models]
        assert names == ["User", "Order", "Item"]

    def test_table_names(self):
        by_name = {m.class_name: m for m in self.models}
        assert by_name["User"].table_name == "users"
        assert by_name["Order"].table_name == "orders"
        assert by_name["Item"].table_name == "items"

    def test_all_model_definitions_typed(self):
        assert all(isinstance(m, ModelDefinition) for m in self.models)

    def test_line_numbers_positive(self):
        assert all(m.line > 0 for m in self.models)


# ===========================================================================
# User model – columns
# ===========================================================================


class TestUserColumns:
    def setup_method(self):
        models = analyze_model_file(_FIXTURE)
        self.user = next(m for m in models if m.class_name == "User")

    def test_column_count(self):
        assert len(self.user.columns) == 3

    def test_column_names(self):
        names = [c.name for c in self.user.columns]
        assert "id" in names
        assert "username" in names
        assert "email" in names

    def test_id_column_type(self):
        id_col = next(c for c in self.user.columns if c.name == "id")
        assert id_col.col_type == "Integer"
        assert id_col.primary_key is True

    def test_username_unique(self):
        col = next(c for c in self.user.columns if c.name == "username")
        assert col.unique is True
        assert col.nullable is False

    def test_email_index(self):
        col = next(c for c in self.user.columns if c.name == "email")
        assert col.index is True
        assert col.nullable is False

    def test_all_column_defs_typed(self):
        assert all(isinstance(c, ColumnDef) for c in self.user.columns)


# ===========================================================================
# Order model – FK, relationship, and explicit index
# ===========================================================================


class TestOrderForeignKeys:
    def setup_method(self):
        models = analyze_model_file(_FIXTURE)
        self.order = next(m for m in models if m.class_name == "Order")

    def test_has_one_fk(self):
        assert len(self.order.foreign_keys) == 1

    def test_fk_column_name(self):
        assert self.order.foreign_keys[0].column_name == "user_id"

    def test_fk_target(self):
        assert self.order.foreign_keys[0].target == "users.id"

    def test_fk_typed(self):
        assert all(isinstance(fk, FKDef) for fk in self.order.foreign_keys)


class TestOrderRelationships:
    def setup_method(self):
        models = analyze_model_file(_FIXTURE)
        self.order = next(m for m in models if m.class_name == "Order")

    def test_has_two_relationships(self):
        assert len(self.order.relationships) == 2

    def test_user_relationship(self):
        user_rel = next(r for r in self.order.relationships if r.name == "user")
        assert user_rel.target_model == "User"
        assert user_rel.back_populates == "orders"

    def test_user_relationship_foreign_keys(self):
        user_rel = next(r for r in self.order.relationships if r.name == "user")
        assert user_rel.foreign_keys == ["user_id"]

    def test_items_relationship(self):
        items_rel = next(r for r in self.order.relationships if r.name == "items")
        assert items_rel.target_model == "Item"
        assert items_rel.back_populates == "order"

    def test_all_rel_defs_typed(self):
        assert all(isinstance(r, RelDef) for r in self.order.relationships)


class TestOrderIndexes:
    def setup_method(self):
        models = analyze_model_file(_FIXTURE)
        self.order = next(m for m in models if m.class_name == "Order")

    def test_has_one_explicit_index(self):
        # ix_orders_user_status defined in __table_args__
        assert len(self.order.indexes) == 1

    def test_index_name(self):
        assert self.order.indexes[0].name == "ix_orders_user_status"

    def test_index_columns(self):
        assert self.order.indexes[0].columns == ["user_id", "status"]

    def test_index_not_unique(self):
        assert self.order.indexes[0].is_unique is False

    def test_index_typed(self):
        assert all(isinstance(i, IndexDef) for i in self.order.indexes)


class TestOrderColumns:
    def setup_method(self):
        models = analyze_model_file(_FIXTURE)
        self.order = next(m for m in models if m.class_name == "Order")

    def test_column_count(self):
        assert len(self.order.columns) == 4

    def test_user_id_not_nullable(self):
        col = next(c for c in self.order.columns if c.name == "user_id")
        assert col.nullable is False

    def test_created_at_has_index(self):
        col = next(c for c in self.order.columns if c.name == "created_at")
        assert col.index is True


# ===========================================================================
# Item model – FK with index=True on the FK column
# ===========================================================================


class TestItemModel:
    def setup_method(self):
        models = analyze_model_file(_FIXTURE)
        self.item = next(m for m in models if m.class_name == "Item")

    def test_has_one_fk(self):
        assert len(self.item.foreign_keys) == 1

    def test_fk_target(self):
        assert self.item.foreign_keys[0].target == "orders.id"

    def test_order_id_has_index(self):
        col = next(c for c in self.item.columns if c.name == "order_id")
        assert col.index is True
        assert col.nullable is False

    def test_has_one_relationship(self):
        assert len(self.item.relationships) == 1
        assert self.item.relationships[0].name == "order"


# ===========================================================================
# analyze_model_source – inline source strings
# ===========================================================================


class TestAnalyzeModelSourceSingleClass:
    """Inline source with one simple model class."""

    SOURCE = """\
class Product(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    price = Column(Integer, nullable=False, index=True)
"""

    def setup_method(self):
        self.models = analyze_model_source(self.SOURCE)

    def test_returns_one_model(self):
        assert len(self.models) == 1

    def test_class_and_table_name(self):
        m = self.models[0]
        assert m.class_name == "Product"
        assert m.table_name == "products"

    def test_three_columns(self):
        assert len(self.models[0].columns) == 3

    def test_id_primary_key(self):
        col = next(c for c in self.models[0].columns if c.name == "id")
        assert col.primary_key is True

    def test_name_unique_not_nullable(self):
        col = next(c for c in self.models[0].columns if c.name == "name")
        assert col.unique is True
        assert col.nullable is False

    def test_price_index(self):
        col = next(c for c in self.models[0].columns if c.name == "price")
        assert col.index is True


class TestAnalyzeModelSourceMultipleClasses:
    """Inline source with two model classes."""

    SOURCE = """\
class Author(Base):
    __tablename__ = "authors"
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    books = relationship("Book", back_populates="author")

class Book(Base):
    __tablename__ = "books"
    id = Column(Integer, primary_key=True)
    author_id = Column(Integer, ForeignKey("authors.id"), nullable=False)
    title = Column(String(200), nullable=False)
    author = relationship("Author", back_populates="books")
"""

    def setup_method(self):
        self.models = analyze_model_source(self.SOURCE)
        self.by_name = {m.class_name: m for m in self.models}

    def test_returns_two_models(self):
        assert len(self.models) == 2

    def test_author_relationship(self):
        rel = self.by_name["Author"].relationships[0]
        assert rel.name == "books"
        assert rel.target_model == "Book"
        assert rel.back_populates == "author"

    def test_book_fk(self):
        fk = self.by_name["Book"].foreign_keys[0]
        assert fk.column_name == "author_id"
        assert fk.target == "authors.id"

    def test_book_relationship_back_populates(self):
        rel = next(r for r in self.by_name["Book"].relationships if r.name == "author")
        assert rel.back_populates == "books"


class TestAnalyzeModelSourceExplicitIndex:
    """Inline source with a unique composite index in __table_args__."""

    SOURCE = """\
class Membership(Base):
    __tablename__ = "memberships"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    group_id = Column(Integer, ForeignKey("groups.id"), nullable=False)

    __table_args__ = (
        Index("ix_membership_user_group", "user_id", "group_id", unique=True),
    )
"""

    def setup_method(self):
        self.models = analyze_model_source(self.SOURCE)

    def test_one_model(self):
        assert len(self.models) == 1

    def test_two_fks(self):
        assert len(self.models[0].foreign_keys) == 2

    def test_unique_index(self):
        idx = self.models[0].indexes[0]
        assert idx.name == "ix_membership_user_group"
        assert idx.columns == ["user_id", "group_id"]
        assert idx.is_unique is True


# ===========================================================================
# Edge cases
# ===========================================================================


class TestEmptySource:
    def test_empty_string(self):
        assert analyze_model_source("") == []

    def test_whitespace_only(self):
        assert analyze_model_source("   \n\t  ") == []


class TestNoModelClasses:
    def test_plain_python_file(self):
        source = """\
def hello():
    return "world"

x = 1 + 2
"""
        assert analyze_model_source(source) == []

    def test_class_without_base(self):
        source = """\
class Standalone:
    id = Column(Integer, primary_key=True)
"""
        assert analyze_model_source(source) == []


class TestClassWithoutTablename:
    """A class inheriting from Base but no __tablename__ is skipped."""

    def test_skipped(self):
        source = """\
class AbstractBase(Base):
    id = Column(Integer, primary_key=True)
"""
        result = analyze_model_source(source)
        assert result == []


class TestDeclarativeBaseSubclass:
    """class Base(DeclarativeBase) is itself skipped (no __tablename__)."""

    def test_base_class_skipped(self):
        source = """\
class Base(DeclarativeBase):
    pass

class Product(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True)
"""
        result = analyze_model_source(source)
        assert len(result) == 1
        assert result[0].class_name == "Product"
