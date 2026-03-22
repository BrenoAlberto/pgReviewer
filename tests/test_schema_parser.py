"""Unit tests for pgreviewer.analysis.schema_parser.

Covers:
- parse_stats_comments: single table, multiple tables, missing fields, empty
- parse_ddl: CREATE TABLE, CREATE INDEX (unique, partial, INCLUDE, GIN),
  column types, schema-qualified names
- merge_schema: stats priority, DDL-only fallback, disjoint tables
- parse_schema_file: end-to-end with combined DDL + stats
"""

from __future__ import annotations

import pytest

from pgreviewer.analysis.schema_parser import (
    merge_schema,
    parse_ddl,
    parse_schema_file,
    parse_stats_comments,
)
from pgreviewer.core.models import ColumnInfo, IndexInfo, SchemaInfo, TableInfo

# ---------------------------------------------------------------------------
# parse_stats_comments
# ---------------------------------------------------------------------------


class TestParseStatsComments:
    def test_single_table(self):
        text = (
            '-- pgreviewer:stats {"orders":{"row_estimate":50000,'
            '"size_bytes":4096000,"indexes":[{"name":"ix_user","columns":["user_id"],'
            '"include_columns":[],"is_unique":false,"is_partial":false,'
            '"index_type":"btree"}],"columns":[{"name":"user_id","type":"integer",'
            '"null_fraction":0.01,"distinct_count":500.0}]}}'
        )
        schema = parse_stats_comments(text)

        assert "orders" in schema.tables
        t = schema.tables["orders"]
        assert t.row_estimate == 50000
        assert t.size_bytes == 4096000
        assert len(t.indexes) == 1
        assert t.indexes[0].name == "ix_user"
        assert t.indexes[0].columns == ["user_id"]
        assert t.indexes[0].is_unique is False
        assert len(t.columns) == 1
        assert t.columns[0].name == "user_id"
        assert t.columns[0].null_fraction == pytest.approx(0.01)

    def test_multiple_tables(self):
        text = (
            '-- pgreviewer:stats {"alpha":{"row_estimate":100,"size_bytes":8192,'
            '"indexes":[],"columns":[]}}\n'
            '-- pgreviewer:stats {"beta":{"row_estimate":200,"size_bytes":16384,'
            '"indexes":[],"columns":[]}}'
        )
        schema = parse_stats_comments(text)
        assert len(schema.tables) == 2
        assert schema.tables["alpha"].row_estimate == 100
        assert schema.tables["beta"].row_estimate == 200

    def test_missing_optional_fields_use_defaults(self):
        text = '-- pgreviewer:stats {"t":{"row_estimate":10}}'
        schema = parse_stats_comments(text)
        t = schema.tables["t"]
        assert t.size_bytes == 0
        assert t.indexes == []
        assert t.columns == []

    def test_empty_input(self):
        schema = parse_stats_comments("")
        assert schema.tables == {}

    def test_non_stats_lines_are_ignored(self):
        text = (
            "CREATE TABLE foo (id int);\n"
            "-- pgreviewer:meta schema dump with statistics\n"
            '-- pgreviewer:stats {"foo":{"row_estimate":42,"size_bytes":0,'
            '"indexes":[],"columns":[]}}\n'
            "-- just a comment\n"
        )
        schema = parse_stats_comments(text)
        assert len(schema.tables) == 1
        assert schema.tables["foo"].row_estimate == 42

    def test_index_with_include_columns(self):
        text = (
            '-- pgreviewer:stats {"t":{"row_estimate":0,"size_bytes":0,'
            '"indexes":[{"name":"ix_covering","columns":["a","b"],'
            '"include_columns":["c"],"is_unique":true,"is_partial":false,'
            '"index_type":"btree"}],"columns":[]}}'
        )
        schema = parse_stats_comments(text)
        idx = schema.tables["t"].indexes[0]
        assert idx.columns == ["a", "b"]
        assert idx.include_columns == ["c"]
        assert idx.is_unique is True

    def test_partial_index(self):
        text = (
            '-- pgreviewer:stats {"t":{"row_estimate":0,"size_bytes":0,'
            '"indexes":[{"name":"ix_active","columns":["status"],'
            '"include_columns":[],"is_unique":false,"is_partial":true,'
            '"index_type":"btree"}],"columns":[]}}'
        )
        schema = parse_stats_comments(text)
        assert schema.tables["t"].indexes[0].is_partial is True


# ---------------------------------------------------------------------------
# parse_ddl
# ---------------------------------------------------------------------------


class TestParseDDL:
    def test_create_table_columns(self):
        ddl = """
        CREATE TABLE public.orders (
            id integer NOT NULL,
            user_id integer,
            status character varying(50),
            created_at timestamp without time zone DEFAULT now()
        );
        """
        schema = parse_ddl(ddl)
        assert "orders" in schema.tables
        cols = {c.name: c for c in schema.tables["orders"].columns}
        assert "id" in cols
        assert cols["id"].type == "integer"
        assert "user_id" in cols
        assert cols["status"].type == "character varying(50)"
        assert cols["created_at"].type == "timestamp without time zone"

    def test_create_table_without_schema_prefix(self):
        ddl = "CREATE TABLE users (id serial NOT NULL, name text);"
        schema = parse_ddl(ddl)
        assert "users" in schema.tables
        cols = {c.name for c in schema.tables["users"].columns}
        assert cols == {"id", "name"}

    def test_create_index_basic(self):
        ddl = """
        CREATE TABLE public.orders (id integer);
        CREATE INDEX ix_orders_user_id ON public.orders USING btree (user_id);
        """
        schema = parse_ddl(ddl)
        indexes = schema.tables["orders"].indexes
        assert len(indexes) == 1
        assert indexes[0].name == "ix_orders_user_id"
        assert indexes[0].columns == ["user_id"]
        assert indexes[0].index_type == "btree"
        assert indexes[0].is_unique is False

    def test_create_unique_index(self):
        ddl = """
        CREATE TABLE public.users (id integer);
        CREATE UNIQUE INDEX users_email_key ON public.users USING btree (email);
        """
        schema = parse_ddl(ddl)
        idx = schema.tables["users"].indexes[0]
        assert idx.is_unique is True
        assert idx.name == "users_email_key"

    def test_create_index_with_include(self):
        ddl = (
            "CREATE TABLE public.orders (id integer);\n"
            "CREATE INDEX ix_covering ON public.orders"
            " USING btree (user_id) INCLUDE (status);\n"
        )
        schema = parse_ddl(ddl)
        idx = schema.tables["orders"].indexes[0]
        assert idx.columns == ["user_id"]
        assert idx.include_columns == ["status"]

    def test_create_partial_index(self):
        ddl = (
            "CREATE TABLE public.orders (id integer);\n"
            "CREATE INDEX ix_active ON public.orders"
            " USING btree (status) WHERE (status = 'active');\n"
        )
        schema = parse_ddl(ddl)
        idx = schema.tables["orders"].indexes[0]
        assert idx.is_partial is True
        assert idx.columns == ["status"]

    def test_create_gin_index(self):
        ddl = """
        CREATE TABLE public.docs (id integer);
        CREATE INDEX ix_docs_body ON public.docs USING gin (body);
        """
        schema = parse_ddl(ddl)
        idx = schema.tables["docs"].indexes[0]
        assert idx.index_type == "gin"

    def test_composite_index(self):
        ddl = """
        CREATE TABLE public.orders (id integer);
        CREATE INDEX ix_composite ON public.orders USING btree (user_id, created_at);
        """
        schema = parse_ddl(ddl)
        idx = schema.tables["orders"].indexes[0]
        assert idx.columns == ["user_id", "created_at"]

    def test_constraint_keywords_not_parsed_as_columns(self):
        ddl = """
        CREATE TABLE public.orders (
            id integer NOT NULL,
            CONSTRAINT orders_pkey PRIMARY KEY (id)
        );
        """
        schema = parse_ddl(ddl)
        col_names = {c.name for c in schema.tables["orders"].columns}
        assert "CONSTRAINT" not in col_names
        assert "id" in col_names

    def test_empty_ddl(self):
        schema = parse_ddl("")
        assert schema.tables == {}

    def test_ddl_defaults_have_zero_stats(self):
        ddl = "CREATE TABLE t (id integer);"
        schema = parse_ddl(ddl)
        assert schema.tables["t"].row_estimate == 0
        assert schema.tables["t"].size_bytes == 0

    def test_array_column_type(self):
        ddl = "CREATE TABLE t (tags text[], scores integer[]);"
        schema = parse_ddl(ddl)
        cols = {c.name: c for c in schema.tables["t"].columns}
        assert cols["tags"].type == "text[]"
        assert cols["scores"].type == "integer[]"

    def test_double_precision_type(self):
        ddl = "CREATE TABLE t (amount double precision);"
        schema = parse_ddl(ddl)
        cols = {c.name: c for c in schema.tables["t"].columns}
        assert cols["amount"].type == "double precision"

    def test_index_on_table_not_in_create_table(self):
        ddl = "CREATE INDEX ix_foo ON public.bar USING btree (col);"
        schema = parse_ddl(ddl)
        assert "bar" in schema.tables
        assert len(schema.tables["bar"].indexes) == 1


# ---------------------------------------------------------------------------
# merge_schema
# ---------------------------------------------------------------------------


class TestMergeSchema:
    def test_stats_takes_priority_for_numerics(self):
        stats = SchemaInfo(
            tables={
                "orders": TableInfo(
                    row_estimate=50000,
                    size_bytes=4096000,
                    indexes=[IndexInfo(name="ix_user", columns=["user_id"])],
                    columns=[
                        ColumnInfo(
                            name="user_id",
                            type="integer",
                            null_fraction=0.01,
                        )
                    ],
                )
            }
        )
        ddl = SchemaInfo(
            tables={
                "orders": TableInfo(
                    row_estimate=0,
                    size_bytes=0,
                    columns=[
                        ColumnInfo(name="user_id", type="integer"),
                        ColumnInfo(name="status", type="text"),
                    ],
                )
            }
        )
        merged = merge_schema(stats, ddl)

        t = merged.tables["orders"]
        assert t.row_estimate == 50000
        assert t.size_bytes == 4096000
        # Stats columns win (they have stats data)
        assert len(t.columns) == 1
        assert t.columns[0].null_fraction == pytest.approx(0.01)
        # Stats indexes win
        assert len(t.indexes) == 1

    def test_ddl_only_tables_included(self):
        stats = SchemaInfo(tables={})
        ddl = SchemaInfo(
            tables={"users": TableInfo(columns=[ColumnInfo(name="id", type="integer")])}
        )
        merged = merge_schema(stats, ddl)
        assert "users" in merged.tables
        assert merged.tables["users"].columns[0].name == "id"

    def test_disjoint_tables_all_included(self):
        stats = SchemaInfo(tables={"orders": TableInfo(row_estimate=1000)})
        ddl = SchemaInfo(tables={"users": TableInfo(row_estimate=0)})
        merged = merge_schema(stats, ddl)
        assert "orders" in merged.tables
        assert "users" in merged.tables

    def test_ddl_columns_used_when_stats_has_none(self):
        stats = SchemaInfo(
            tables={
                "t": TableInfo(
                    row_estimate=100,
                    columns=[],  # No column stats
                )
            }
        )
        ddl = SchemaInfo(
            tables={
                "t": TableInfo(
                    columns=[
                        ColumnInfo(name="id", type="integer"),
                        ColumnInfo(name="name", type="text"),
                    ],
                )
            }
        )
        merged = merge_schema(stats, ddl)
        # DDL columns used because stats has empty list
        assert len(merged.tables["t"].columns) == 2
        # But stats row_estimate is preserved
        assert merged.tables["t"].row_estimate == 100


# ---------------------------------------------------------------------------
# parse_schema_file (end-to-end)
# ---------------------------------------------------------------------------


class TestParseSchemaFile:
    def test_combined_ddl_and_stats(self, tmp_path):
        schema_sql = tmp_path / "schema.sql"
        schema_sql.write_text(
            "CREATE TABLE public.orders (\n"
            "    id integer NOT NULL,\n"
            "    user_id integer,\n"
            "    status character varying(50)\n"
            ");\n"
            "\n"
            "CREATE INDEX ix_orders_user_id ON public.orders"
            " USING btree (user_id);\n"
            "\n"
            "-- pgreviewer:meta schema dump with statistics\n"
            '-- pgreviewer:stats {"orders":{"row_estimate":50000,'
            '"size_bytes":4096000,"indexes":[{"name":"ix_orders_user_id",'
            '"columns":["user_id"],"include_columns":[],"is_unique":false,'
            '"is_partial":false,"index_type":"btree"}],"columns":'
            '[{"name":"user_id","type":"integer","null_fraction":0.01,'
            '"distinct_count":500.0}]}}\n'
        )

        schema = parse_schema_file(schema_sql)

        assert "orders" in schema.tables
        t = schema.tables["orders"]
        # Stats data takes priority
        assert t.row_estimate == 50000
        assert t.size_bytes == 4096000
        # Stats indexes
        assert len(t.indexes) == 1
        assert t.indexes[0].name == "ix_orders_user_id"
        # Stats columns
        assert len(t.columns) == 1
        assert t.columns[0].null_fraction == pytest.approx(0.01)

    def test_ddl_only_no_stats(self, tmp_path):
        schema_sql = tmp_path / "schema.sql"
        schema_sql.write_text(
            "CREATE TABLE public.users (\n"
            "    id integer NOT NULL,\n"
            "    email text\n"
            ");\n"
            "\n"
            "CREATE UNIQUE INDEX users_email_key ON public.users"
            " USING btree (email);\n"
        )

        schema = parse_schema_file(schema_sql)

        assert "users" in schema.tables
        t = schema.tables["users"]
        assert t.row_estimate == 0  # No stats, default
        cols = {c.name for c in t.columns}
        assert cols == {"id", "email"}
        assert len(t.indexes) == 1
        assert t.indexes[0].is_unique is True

    def test_stats_only_no_ddl(self, tmp_path):
        schema_sql = tmp_path / "schema.sql"
        schema_sql.write_text(
            '-- pgreviewer:stats {"orders":{"row_estimate":1000,'
            '"size_bytes":8192,"indexes":[],"columns":[]}}\n'
        )

        schema = parse_schema_file(schema_sql)
        assert schema.tables["orders"].row_estimate == 1000

    def test_schema_info_usable_by_fk_detector(self, tmp_path):
        """The parsed SchemaInfo works with the FK-without-index detector."""
        schema_sql = tmp_path / "schema.sql"
        schema_sql.write_text(
            '-- pgreviewer:stats {"orders":{"row_estimate":50000,'
            '"size_bytes":4096000,"indexes":[{"name":"ix_orders_user_id",'
            '"columns":["user_id"],"include_columns":[],"is_unique":false,'
            '"is_partial":false,"index_type":"btree"}],"columns":[]}}\n'
        )

        schema = parse_schema_file(schema_sql)

        # Simulate what FKWithoutIndexDetector._is_indexed does:
        # Check if FK column 'user_id' is a prefix of any index columns
        table_info = schema.tables.get("orders")
        assert table_info is not None
        fk_cols = ["user_id"]
        found = any(
            idx.columns[: len(fk_cols)] == fk_cols
            for idx in table_info.indexes
            if len(idx.columns) >= len(fk_cols)
        )
        assert found is True

    def test_schema_info_usable_by_severity_escalation(self, tmp_path):
        """Row estimate enables severity escalation in detectors."""
        schema_sql = tmp_path / "schema.sql"
        schema_sql.write_text(
            '-- pgreviewer:stats {"orders":{"row_estimate":500000,'
            '"size_bytes":40960000,"indexes":[],"columns":[]}}\n'
        )

        schema = parse_schema_file(schema_sql)

        # Simulate detector severity escalation logic
        schema_available = bool(schema.tables)
        assert schema_available is True
        table_info = schema.tables.get("orders")
        assert table_info is not None
        assert table_info.row_estimate > 100_000  # CRITICAL threshold
