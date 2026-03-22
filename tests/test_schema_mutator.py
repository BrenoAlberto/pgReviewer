"""Unit tests for pgreviewer.analysis.schema_mutator.

Covers:
- _apply_create_table: new table, columns, IF NOT EXISTS, schema-qualified
- _apply_create_index: basic, unique, partial, GIN, INCLUDE, CONCURRENTLY
- _apply_add_column: single, multiple, IF NOT EXISTS, quoted names
- mutate_schema: ordering, multiple statements, immutability of base
"""

from __future__ import annotations

from pgreviewer.analysis.schema_mutator import mutate_schema
from pgreviewer.core.models import (
    ColumnInfo,
    DDLStatement,
    IndexInfo,
    SchemaInfo,
    TableInfo,
)


def _stmt(raw_sql: str, statement_type: str, table: str | None = None) -> DDLStatement:
    return DDLStatement(
        statement_type=statement_type,
        table=table,
        raw_sql=raw_sql,
        line_number=1,
    )


# ---------------------------------------------------------------------------
# CREATE TABLE
# ---------------------------------------------------------------------------


class TestApplyCreateTable:
    def test_new_table_with_columns(self):
        base = SchemaInfo()
        stmts = [
            _stmt(
                "CREATE TABLE orders ("
                "id integer NOT NULL, "
                "user_id integer, "
                "status character varying(50)"
                ");",
                "CREATE TABLE",
                "orders",
            )
        ]
        result = mutate_schema(base, stmts)
        assert "orders" in result.tables
        cols = {c.name for c in result.tables["orders"].columns}
        assert cols == {"id", "user_id", "status"}

    def test_schema_qualified_name(self):
        base = SchemaInfo()
        stmts = [
            _stmt(
                "CREATE TABLE public.users (id serial NOT NULL, email text);",
                "CREATE TABLE",
                "public.users",
            )
        ]
        result = mutate_schema(base, stmts)
        assert "users" in result.tables

    def test_if_not_exists(self):
        base = SchemaInfo()
        stmts = [
            _stmt(
                "CREATE TABLE IF NOT EXISTS orders (id integer);",
                "CREATE TABLE",
                "orders",
            )
        ]
        result = mutate_schema(base, stmts)
        assert "orders" in result.tables

    def test_adds_columns_to_existing_table(self):
        base = SchemaInfo(
            tables={
                "orders": TableInfo(columns=[ColumnInfo(name="id", type="integer")])
            }
        )
        stmts = [
            _stmt(
                "CREATE TABLE orders (id integer, status text);",
                "CREATE TABLE",
                "orders",
            )
        ]
        result = mutate_schema(base, stmts)
        cols = {c.name for c in result.tables["orders"].columns}
        assert "id" in cols
        assert "status" in cols

    def test_skips_constraint_keywords(self):
        stmts = [
            _stmt(
                "CREATE TABLE orders ("
                "id integer NOT NULL, "
                "CONSTRAINT orders_pkey PRIMARY KEY (id)"
                ");",
                "CREATE TABLE",
                "orders",
            )
        ]
        result = mutate_schema(SchemaInfo(), stmts)
        col_names = {c.name for c in result.tables["orders"].columns}
        assert "CONSTRAINT" not in col_names
        assert "id" in col_names

    def test_fallback_to_table_hint(self):
        stmts = [
            _stmt(
                "CREATE TABLE IF NOT EXISTS orders AS SELECT 1;",
                "CREATE TABLE",
                "orders",
            )
        ]
        result = mutate_schema(SchemaInfo(), stmts)
        assert "orders" in result.tables

    def test_multiline_create_table(self):
        sql = (
            "CREATE TABLE public.orders (\n"
            "    id integer NOT NULL,\n"
            "    user_id integer,\n"
            "    created_at timestamp without time zone DEFAULT now()\n"
            ");"
        )
        stmts = [_stmt(sql, "CREATE TABLE", "public.orders")]
        result = mutate_schema(SchemaInfo(), stmts)
        cols = {c.name: c for c in result.tables["orders"].columns}
        assert "id" in cols
        assert "user_id" in cols
        assert cols["created_at"].type == "timestamp without time zone"


# ---------------------------------------------------------------------------
# CREATE INDEX
# ---------------------------------------------------------------------------


class TestApplyCreateIndex:
    def test_basic_index(self):
        base = SchemaInfo(tables={"orders": TableInfo()})
        stmts = [
            _stmt(
                "CREATE INDEX ix_orders_user ON public.orders USING btree (user_id);",
                "CREATE INDEX",
            )
        ]
        result = mutate_schema(base, stmts)
        idx = result.tables["orders"].indexes[0]
        assert idx.name == "ix_orders_user"
        assert idx.columns == ["user_id"]
        assert idx.index_type == "btree"
        assert idx.is_unique is False

    def test_unique_index(self):
        stmts = [
            _stmt(
                "CREATE UNIQUE INDEX users_email_key ON users USING btree (email);",
                "CREATE INDEX",
            )
        ]
        result = mutate_schema(SchemaInfo(), stmts)
        assert result.tables["users"].indexes[0].is_unique is True

    def test_partial_index(self):
        stmts = [
            _stmt(
                "CREATE INDEX ix_active ON orders"
                " USING btree (status) WHERE (status = 'active');",
                "CREATE INDEX",
            )
        ]
        result = mutate_schema(SchemaInfo(), stmts)
        assert result.tables["orders"].indexes[0].is_partial is True

    def test_gin_index(self):
        stmts = [
            _stmt(
                "CREATE INDEX ix_body ON docs USING gin (body);",
                "CREATE INDEX",
            )
        ]
        result = mutate_schema(SchemaInfo(), stmts)
        assert result.tables["docs"].indexes[0].index_type == "gin"

    def test_include_columns(self):
        stmts = [
            _stmt(
                "CREATE INDEX ix_covering ON orders"
                " USING btree (user_id) INCLUDE (status, created_at);",
                "CREATE INDEX",
            )
        ]
        result = mutate_schema(SchemaInfo(), stmts)
        idx = result.tables["orders"].indexes[0]
        assert idx.columns == ["user_id"]
        assert idx.include_columns == ["status", "created_at"]

    def test_concurrently(self):
        stmts = [
            _stmt(
                "CREATE INDEX CONCURRENTLY ix_user ON orders USING btree (user_id);",
                "CREATE INDEX",
            )
        ]
        result = mutate_schema(SchemaInfo(), stmts)
        assert result.tables["orders"].indexes[0].name == "ix_user"

    def test_creates_table_entry_if_missing(self):
        stmts = [
            _stmt(
                "CREATE INDEX ix_foo ON bar USING btree (col);",
                "CREATE INDEX",
            )
        ]
        result = mutate_schema(SchemaInfo(), stmts)
        assert "bar" in result.tables
        assert len(result.tables["bar"].indexes) == 1


# ---------------------------------------------------------------------------
# ALTER TABLE ADD COLUMN
# ---------------------------------------------------------------------------


class TestApplyAddColumn:
    def test_single_add_column(self):
        base = SchemaInfo(tables={"orders": TableInfo()})
        stmts = [
            _stmt(
                "ALTER TABLE orders ADD COLUMN status text;",
                "ALTER TABLE",
                "orders",
            )
        ]
        result = mutate_schema(base, stmts)
        cols = {c.name for c in result.tables["orders"].columns}
        assert "status" in cols

    def test_add_column_if_not_exists(self):
        base = SchemaInfo(tables={"orders": TableInfo()})
        stmts = [
            _stmt(
                "ALTER TABLE orders ADD COLUMN IF NOT EXISTS status text;",
                "ALTER TABLE",
                "orders",
            )
        ]
        result = mutate_schema(base, stmts)
        cols = {c.name for c in result.tables["orders"].columns}
        assert "status" in cols

    def test_does_not_duplicate_existing_column(self):
        base = SchemaInfo(
            tables={
                "orders": TableInfo(columns=[ColumnInfo(name="status", type="text")])
            }
        )
        stmts = [
            _stmt(
                "ALTER TABLE orders ADD COLUMN status text;",
                "ALTER TABLE",
                "orders",
            )
        ]
        result = mutate_schema(base, stmts)
        assert len(result.tables["orders"].columns) == 1

    def test_schema_qualified_table(self):
        stmts = [
            _stmt(
                "ALTER TABLE public.orders ADD COLUMN status text;",
                "ALTER TABLE",
                "public.orders",
            )
        ]
        result = mutate_schema(SchemaInfo(), stmts)
        assert "orders" in result.tables

    def test_quoted_column_name(self):
        stmts = [
            _stmt(
                'ALTER TABLE orders ADD COLUMN "Order Status" text;',
                "ALTER TABLE",
                "orders",
            )
        ]
        result = mutate_schema(SchemaInfo(), stmts)
        cols = {c.name for c in result.tables["orders"].columns}
        assert "Order Status" in cols


# ---------------------------------------------------------------------------
# mutate_schema integration
# ---------------------------------------------------------------------------


class TestMutateSchema:
    def test_base_schema_not_mutated(self):
        base = SchemaInfo(
            tables={
                "orders": TableInfo(
                    row_estimate=50000,
                    columns=[ColumnInfo(name="id", type="integer")],
                )
            }
        )
        stmts = [
            _stmt(
                "ALTER TABLE orders ADD COLUMN status text;",
                "ALTER TABLE",
                "orders",
            )
        ]
        result = mutate_schema(base, stmts)
        # Base must be unmodified
        assert len(base.tables["orders"].columns) == 1
        # Result has new column
        assert len(result.tables["orders"].columns) == 2

    def test_preserves_existing_stats(self):
        base = SchemaInfo(
            tables={
                "orders": TableInfo(
                    row_estimate=50000,
                    size_bytes=4096000,
                    indexes=[IndexInfo(name="ix_existing", columns=["id"])],
                )
            }
        )
        stmts = [
            _stmt(
                "CREATE INDEX ix_new ON orders USING btree (user_id);",
                "CREATE INDEX",
            )
        ]
        result = mutate_schema(base, stmts)
        t = result.tables["orders"]
        assert t.row_estimate == 50000
        assert t.size_bytes == 4096000
        assert len(t.indexes) == 2

    def test_multiple_statements_in_order(self):
        stmts = [
            _stmt(
                "CREATE TABLE orders (id integer);",
                "CREATE TABLE",
                "orders",
            ),
            _stmt(
                "ALTER TABLE orders ADD COLUMN user_id integer;",
                "ALTER TABLE",
                "orders",
            ),
            _stmt(
                "CREATE INDEX ix_orders_user ON orders USING btree (user_id);",
                "CREATE INDEX",
            ),
        ]
        result = mutate_schema(SchemaInfo(), stmts)
        t = result.tables["orders"]
        cols = {c.name for c in t.columns}
        assert cols == {"id", "user_id"}
        assert len(t.indexes) == 1
        assert t.indexes[0].columns == ["user_id"]

    def test_empty_statements(self):
        base = SchemaInfo(tables={"t": TableInfo(row_estimate=100)})
        result = mutate_schema(base, [])
        assert result.tables["t"].row_estimate == 100

    def test_unknown_statement_type_ignored(self):
        stmts = [
            _stmt("DROP TABLE orders;", "DROP TABLE", "orders"),
        ]
        result = mutate_schema(SchemaInfo(), stmts)
        # DROP TABLE is not handled by mutator, should not crash
        assert result.tables == {}

    def test_cross_file_scenario(self):
        """FK in file 0001, index in file 0002 — both apply to same schema."""
        stmts = [
            _stmt(
                "CREATE TABLE orders (id integer NOT NULL, user_id integer);",
                "CREATE TABLE",
                "orders",
            ),
            _stmt(
                "CREATE INDEX ix_orders_user ON orders USING btree (user_id);",
                "CREATE INDEX",
            ),
        ]
        result = mutate_schema(SchemaInfo(), stmts)
        t = result.tables["orders"]
        # Verify FK column user_id is indexed (simulates detector check)
        fk_cols = ["user_id"]
        found = any(
            idx.columns[: len(fk_cols)] == fk_cols
            for idx in t.indexes
            if len(idx.columns) >= len(fk_cols)
        )
        assert found is True
