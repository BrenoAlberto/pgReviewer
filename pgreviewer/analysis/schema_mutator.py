"""Apply DDL changes from a PR diff to an existing :class:`SchemaInfo`.

When a PR adds new tables, indexes, or columns via migration DDL, those
changes must be reflected in the schema *before* detectors run — otherwise
detectors cannot see the post-merge state and produce false positives
(e.g. flagging a missing index that the same PR creates in a later file).

The :func:`mutate_schema` function takes a base :class:`SchemaInfo` (from
``.pgreviewer/schema.sql`` or an empty one) and a list of
:class:`~pgreviewer.core.models.DDLStatement` objects extracted from the
diff, then returns a new :class:`SchemaInfo` reflecting the schema as it
will be after the PR merges.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from pgreviewer.core.models import ColumnInfo, IndexInfo, SchemaInfo, TableInfo

if TYPE_CHECKING:
    from pgreviewer.core.models import DDLStatement

# ---------------------------------------------------------------------------
# Regex patterns for DDL extraction
# ---------------------------------------------------------------------------

# CREATE TABLE [schema.]name (body);
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:\w+\.)?(?P<table>\w+)\s*\("
    r"(?P<body>[^;]+)\);",
    re.IGNORECASE | re.DOTALL,
)

# Column definition (applied per-element after comma-splitting the body)
_COLUMN_DEF_RE = re.compile(
    r"^\s*(?P<name>\w+)\s+"
    r"(?P<type>"
    r"(?:character\s+varying|double\s+precision"
    r"|timestamp\s+with(?:out)?\s+time\s+zone"
    r"|time\s+with(?:out)?\s+time\s+zone|\w+)"
    r"(?:\([^)]*\))?(?:\[\])?)",
    re.IGNORECASE | re.DOTALL,
)

# CREATE [UNIQUE] INDEX [CONCURRENTLY] name ON [schema.]table
#   [USING method] (columns) [INCLUDE (columns)] [WHERE ...]
_CREATE_INDEX_RE = re.compile(
    r"CREATE\s+(?P<unique>UNIQUE\s+)?INDEX\s+"
    r"(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<name>[^\s(]+)\s+ON\s+"
    r"(?:ONLY\s+)?(?:\w+\.)?(?P<table>\w+)\s+"
    r"(?:USING\s+(?P<method>\w+)\s+)?"
    r"\((?P<columns>[^)]+)\)"
    r"(?:\s+INCLUDE\s+\((?P<include>[^)]+)\))?"
    r"(?:\s+WHERE\s+(?P<where>.+?))?;",
    re.IGNORECASE | re.DOTALL,
)

# ALTER TABLE [schema.]table ADD COLUMN name type ...
_ADD_COLUMN_RE = re.compile(
    r"ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<name>(?:\"[^\"]+\"|\w+))\s+"
    r"(?P<type>"
    r"(?:character\s+varying|double\s+precision"
    r"|timestamp\s+with(?:out)?\s+time\s+zone"
    r"|time\s+with(?:out)?\s+time\s+zone|\w+)"
    r"(?:\([^)]*\))?(?:\[\])?)",
    re.IGNORECASE,
)

_SKIP_COLUMN_NAMES = frozenset(
    {
        "CONSTRAINT",
        "PRIMARY",
        "UNIQUE",
        "CHECK",
        "FOREIGN",
    }
)


def _strip_schema(name: str) -> str:
    """Remove schema prefix and quoting: ``public."orders"`` → ``orders``."""
    return name.rsplit(".", 1)[-1].strip('"')


def _parse_columns_list(cols_str: str) -> list[str]:
    return [c.strip().strip('"') for c in cols_str.split(",") if c.strip()]


def _split_body(body: str) -> list[str]:
    """Split CREATE TABLE body on top-level commas (not inside parens)."""
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


# ---------------------------------------------------------------------------
# Mutation functions
# ---------------------------------------------------------------------------


def _apply_create_table(
    schema: SchemaInfo, raw_sql: str, table_hint: str | None
) -> None:
    """Apply a CREATE TABLE statement to *schema*."""
    match = _CREATE_TABLE_RE.search(raw_sql)
    if not match:
        # Fallback to table_hint from parse_ddl_statement
        if table_hint:
            table_name = _strip_schema(table_hint)
            schema.tables.setdefault(table_name, TableInfo())
        return

    table_name = _strip_schema(match.group("table"))
    body = match.group("body")

    columns: list[ColumnInfo] = []
    for part in _split_body(body):
        col_match = _COLUMN_DEF_RE.match(part)
        if not col_match:
            continue
        col_name = col_match.group("name")
        if col_name.upper() in _SKIP_COLUMN_NAMES:
            continue
        columns.append(ColumnInfo(name=col_name, type=col_match.group("type")))

    if table_name in schema.tables:
        # Table exists: add any new columns
        existing = {c.name for c in schema.tables[table_name].columns}
        for col in columns:
            if col.name not in existing:
                schema.tables[table_name].columns.append(col)
    else:
        schema.tables[table_name] = TableInfo(columns=columns)


def _apply_create_index(schema: SchemaInfo, raw_sql: str) -> None:
    """Apply a CREATE INDEX statement to *schema*."""
    match = _CREATE_INDEX_RE.search(raw_sql)
    if not match:
        return

    table_name = _strip_schema(match.group("table"))
    schema.tables.setdefault(table_name, TableInfo())

    columns = _parse_columns_list(match.group("columns"))
    include_raw = match.group("include")
    include_columns = _parse_columns_list(include_raw) if include_raw else []

    schema.tables[table_name].indexes.append(
        IndexInfo(
            name=match.group("name").strip(),
            columns=columns,
            include_columns=include_columns,
            is_unique=bool(match.group("unique")),
            is_partial=bool(match.group("where")),
            index_type=(match.group("method") or "btree").lower(),
        )
    )


def _apply_add_column(schema: SchemaInfo, raw_sql: str, table_name: str) -> None:
    """Apply ALTER TABLE … ADD COLUMN to *schema*."""
    table_name = _strip_schema(table_name)
    schema.tables.setdefault(table_name, TableInfo())

    for match in _ADD_COLUMN_RE.finditer(raw_sql):
        col_name = match.group("name").strip('"')
        col_type = match.group("type")
        existing = {c.name for c in schema.tables[table_name].columns}
        if col_name not in existing:
            schema.tables[table_name].columns.append(
                ColumnInfo(name=col_name, type=col_type)
            )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def mutate_schema(
    base: SchemaInfo,
    statements: list[DDLStatement],
) -> SchemaInfo:
    """Apply DDL statements from a PR diff to a base schema.

    Returns a **new** :class:`SchemaInfo` reflecting the post-merge state.
    The *base* schema is deep-copied so the original is not modified.

    Parameters
    ----------
    base:
        Base schema (e.g. from ``.pgreviewer/schema.sql``).
    statements:
        DDL statements extracted from the PR diff, in file/line order.

    Returns
    -------
    SchemaInfo
        Schema reflecting the state after all DDL in the PR is applied.
    """
    # Deep copy the base schema so we don't mutate the original
    schema = SchemaInfo.model_validate(base.model_dump())

    for stmt in statements:
        stype = stmt.statement_type

        if stype == "CREATE TABLE":
            _apply_create_table(schema, stmt.raw_sql, stmt.table)

        elif stype == "CREATE INDEX":
            _apply_create_index(schema, stmt.raw_sql)

        elif stype == "ALTER TABLE":
            upper = stmt.raw_sql.upper()
            if "ADD COLUMN" in upper and stmt.table:
                _apply_add_column(schema, stmt.raw_sql, stmt.table)

    return schema
