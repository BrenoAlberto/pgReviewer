"""Parse a ``.pgreviewer/schema.sql`` file into :class:`SchemaInfo`.

Two complementary parsers work together:

1. **Stats comment parser** — extracts ``-- pgreviewer:stats`` JSON lines
   written by :func:`~pgreviewer.analysis.schema_dumper.format_stats_comments`.
   This is the primary data source and maps directly to the existing models.

2. **DDL parser** — extracts structural information from ``pg_dump`` DDL
   (``CREATE TABLE``, ``CREATE INDEX``, ``ALTER TABLE … PRIMARY KEY / FOREIGN
   KEY``).  Provides table/column/index data when stats comments are absent.

The final :class:`SchemaInfo` merges both sources: stats take priority for
numeric fields (``row_estimate``, ``size_bytes``, ``null_fraction``, …),
while DDL fills in structural gaps (columns, indexes from tables that have
no stats comment).
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING

from pgreviewer.core.models import ColumnInfo, IndexInfo, SchemaInfo, TableInfo

if TYPE_CHECKING:
    from pathlib import Path


# ---------------------------------------------------------------------------
# Stats comment parser
# ---------------------------------------------------------------------------

_STATS_PREFIX = "-- pgreviewer:stats "


def parse_stats_comments(text: str) -> SchemaInfo:
    """Parse ``-- pgreviewer:stats`` lines into a :class:`SchemaInfo`.

    Each matching line contains a JSON object with a single key (the table
    name) whose value holds ``row_estimate``, ``size_bytes``, ``indexes``,
    and ``columns`` dicts.
    """
    tables: dict[str, TableInfo] = {}

    for line in text.splitlines():
        if not line.startswith(_STATS_PREFIX):
            continue

        payload = json.loads(line[len(_STATS_PREFIX) :])
        for table_name, data in payload.items():
            indexes = [
                IndexInfo(
                    name=idx["name"],
                    columns=idx.get("columns", []),
                    include_columns=idx.get("include_columns", []),
                    is_unique=idx.get("is_unique", False),
                    is_partial=idx.get("is_partial", False),
                    index_type=idx.get("index_type", "btree"),
                )
                for idx in data.get("indexes", [])
            ]
            columns = [
                ColumnInfo(
                    name=col["name"],
                    type=col.get("type", "unknown"),
                    null_fraction=col.get("null_fraction", 0.0),
                    distinct_count=col.get("distinct_count", 0.0),
                )
                for col in data.get("columns", [])
            ]
            tables[table_name] = TableInfo(
                row_estimate=data.get("row_estimate", 0),
                size_bytes=data.get("size_bytes", 0),
                indexes=indexes,
                columns=columns,
            )

    return SchemaInfo(tables=tables)


# ---------------------------------------------------------------------------
# DDL parser
# ---------------------------------------------------------------------------

# CREATE TABLE public.orders ( ... );
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:(?P<schema>\w+)\.)?(?P<table>\w+)\s*\("
    r"(?P<body>[^;]+)\);",
    re.IGNORECASE | re.DOTALL,
)

# Column definition fragment (applied per-element after splitting on commas):
#   column_name  type_name[(args)]  [NOT NULL]  [DEFAULT ...]
_COLUMN_DEF_RE = re.compile(
    r"^\s*(?P<name>\w+)\s+"
    r"(?P<type>"
    r"(?:character\s+varying|double\s+precision|timestamp\s+with(?:out)?\s+time\s+zone"
    r"|time\s+with(?:out)?\s+time\s+zone|\w+)"
    r"(?:\([^)]*\))?(?:\[\])?)",
    re.IGNORECASE | re.DOTALL,
)

# CREATE [UNIQUE] INDEX [CONCURRENTLY] name ON [schema.]table
#   USING method (columns) [INCLUDE (columns)] [WHERE ...]
_CREATE_INDEX_RE = re.compile(
    r"CREATE\s+(?P<unique>UNIQUE\s+)?INDEX\s+"
    r"(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<name>\w+)\s+ON\s+"
    r"(?:ONLY\s+)?(?:\w+\.)?(?P<table>\w+)\s+"
    r"(?:USING\s+(?P<method>\w+)\s+)?"
    r"\((?P<columns>[^)]+)\)"
    r"(?:\s+INCLUDE\s+\((?P<include>[^)]+)\))?"
    r"(?:\s+WHERE\s+(?P<where>.+?))?;",
    re.IGNORECASE | re.DOTALL,
)

# ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY (columns)
_PK_CONSTRAINT_RE = re.compile(
    r"ALTER\s+TABLE\s+(?:ONLY\s+)?(?:\w+\.)?(?P<table>\w+)\s+"
    r"ADD\s+CONSTRAINT\s+\w+\s+"
    r"PRIMARY\s+KEY\s+\((?P<columns>[^)]+)\)",
    re.IGNORECASE,
)


def _split_body(body: str) -> list[str]:
    """Split a CREATE TABLE body on top-level commas (not inside parentheses)."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for char in body:
        if char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    if current:
        parts.append("".join(current))
    return parts


def _parse_columns_list(cols_str: str) -> list[str]:
    """Parse a comma-separated column list, stripping whitespace and quoting."""
    return [c.strip().strip('"') for c in cols_str.split(",") if c.strip()]


def _strip_schema(name: str) -> str:
    """Remove optional schema prefix (e.g. ``public.orders`` → ``orders``)."""
    return name.rsplit(".", 1)[-1].strip('"')


def parse_ddl(text: str) -> SchemaInfo:
    """Parse ``pg_dump`` DDL into a :class:`SchemaInfo`.

    Extracts ``CREATE TABLE``, ``CREATE INDEX``, and
    ``ALTER TABLE … PRIMARY KEY`` statements.  Column types are taken from
    the DDL; statistics fields default to zero.
    """
    tables: dict[str, TableInfo] = {}

    # 1. CREATE TABLE → columns
    for match in _CREATE_TABLE_RE.finditer(text):
        table_name = _strip_schema(match.group("table"))
        body = match.group("body")

        columns: list[ColumnInfo] = []
        # Split body on commas that are NOT inside parentheses
        parts = _split_body(body)
        for part in parts:
            col_match = _COLUMN_DEF_RE.match(part)
            if not col_match:
                continue
            col_name = col_match.group("name")
            # Skip constraint keywords that look like column names
            if col_name.upper() in {
                "CONSTRAINT",
                "PRIMARY",
                "UNIQUE",
                "CHECK",
                "FOREIGN",
            }:
                continue
            col_type = col_match.group("type")
            columns.append(ColumnInfo(name=col_name, type=col_type))

        tables[table_name] = TableInfo(columns=columns)

    # 2. CREATE INDEX → indexes
    for match in _CREATE_INDEX_RE.finditer(text):
        table_name = _strip_schema(match.group("table"))
        if table_name not in tables:
            tables[table_name] = TableInfo()

        columns = _parse_columns_list(match.group("columns"))
        include_raw = match.group("include")
        include_columns = _parse_columns_list(include_raw) if include_raw else []

        tables[table_name].indexes.append(
            IndexInfo(
                name=match.group("name"),
                columns=columns,
                include_columns=include_columns,
                is_unique=bool(match.group("unique")),
                is_partial=bool(match.group("where")),
                index_type=(match.group("method") or "btree").lower(),
            )
        )

    return tables_to_schema(tables)


def tables_to_schema(tables: dict[str, TableInfo]) -> SchemaInfo:
    """Wrap a tables dict into :class:`SchemaInfo`."""
    return SchemaInfo(tables=tables)


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------


def merge_schema(stats: SchemaInfo, ddl: SchemaInfo) -> SchemaInfo:
    """Merge stats-based and DDL-based :class:`SchemaInfo` objects.

    Stats data takes priority for numeric fields.  DDL fills in tables and
    columns that have no stats comment.
    """
    merged: dict[str, TableInfo] = {}

    all_tables = set(stats.tables) | set(ddl.tables)
    for table_name in all_tables:
        stats_info = stats.tables.get(table_name)
        ddl_info = ddl.tables.get(table_name)

        if stats_info and not ddl_info:
            merged[table_name] = stats_info
        elif ddl_info and not stats_info:
            merged[table_name] = ddl_info
        elif stats_info and ddl_info:
            # Stats takes priority for numeric fields; DDL fills structure
            # Use stats columns if available, otherwise DDL columns
            columns = stats_info.columns if stats_info.columns else ddl_info.columns
            # Use stats indexes if available, otherwise DDL indexes
            indexes = stats_info.indexes if stats_info.indexes else ddl_info.indexes

            merged[table_name] = TableInfo(
                row_estimate=stats_info.row_estimate,
                size_bytes=stats_info.size_bytes,
                indexes=indexes,
                columns=columns,
            )

    return SchemaInfo(tables=merged)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_schema_file(path: Path) -> SchemaInfo:
    """Parse a ``.pgreviewer/schema.sql`` file into :class:`SchemaInfo`.

    Combines stats comment data with DDL structural data.  If the file
    contains no stats comments, falls back to DDL-only parsing with
    zero-valued statistics.

    Parameters
    ----------
    path:
        Path to the schema SQL file.

    Returns
    -------
    SchemaInfo
        Populated schema information usable by all detectors.
    """
    text = path.read_text()
    stats_schema = parse_stats_comments(text)
    ddl_schema = parse_ddl(text)
    return merge_schema(stats_schema, ddl_schema)
