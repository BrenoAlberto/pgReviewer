"""Substitute parameterized query placeholders with realistic dummy values."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pgreviewer.core.models import SchemaInfo

# ---------------------------------------------------------------------------
# Placeholder patterns
# ---------------------------------------------------------------------------

# $1, $2, … (PostgreSQL positional)
_POSITIONAL_PG = re.compile(r"\$(\d+)")

# %s (psycopg2 positional) — only bare %s, not %% or %(name)s
_POSITIONAL_PSYCOPG = re.compile(r"(?<!%)%s")

# :param_name (SQLAlchemy named) — negative look-behind avoids ::cast
_NAMED_PARAM = re.compile(r"(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)")

# Column-name = <param> pattern used to discover the column being filtered
_BEFORE_PARAM_RE = re.compile(
    r"([a-zA-Z_][a-zA-Z0-9_.]*)\s*"
    r"(?:=|!=|<>|>=|<=|>|<|(?:NOT\s+)?LIKE|ILIKE)\s*$",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Type sets used for schema-based inference
# ---------------------------------------------------------------------------

_INT_TYPES = frozenset(
    {
        "integer",
        "int",
        "int2",
        "int4",
        "int8",
        "bigint",
        "smallint",
        "serial",
        "bigserial",
    }
)
_TEXT_TYPES = frozenset(
    {
        "text",
        "varchar",
        "character varying",
        "char",
        "character",
        "name",
        "citext",
        "uuid",
    }
)
_DATE_TYPES = frozenset(
    {
        "timestamp",
        "timestamptz",
        "date",
        "time",
        "timetz",
        "timestamp with time zone",
        "timestamp without time zone",
    }
)
_BOOL_TYPES = frozenset({"boolean", "bool"})
_NUMERIC_TYPES = frozenset(
    {"numeric", "decimal", "real", "double precision", "float4", "float8", "money"}
)


# ---------------------------------------------------------------------------
# Dummy-value helpers
# ---------------------------------------------------------------------------


def _dummy_for_col_type(col_type: str) -> str:
    """Return a dummy value that matches *col_type*."""
    t = col_type.lower().split("(")[0].strip()
    if t in _INT_TYPES:
        return "42"
    if t in _TEXT_TYPES:
        return "'placeholder'"
    if t in _DATE_TYPES:
        return "NOW()"
    if t in _BOOL_TYPES:
        return "TRUE"
    if t in _NUMERIC_TYPES:
        return "1.0"
    return "'placeholder'"  # safe default for unrecognised types


_BOOL_NAME_RE = re.compile(
    r"(^is_|^has_|^can_|^was_|active$|enabled$|deleted$|visible$|published$)"
)
_DATE_NAME_RE = re.compile(r"(_at$|_date$|_time$|_on$|^date$|^time$|^timestamp$)")
_ID_NAME_RE = re.compile(r"(^id$|_id$|_ids$)")
_NUMERIC_NAME_RE = re.compile(
    r"(price|amount|cost|balance|rate|count|total|qty|quantity|weight|score|rank)"
)


def _dummy_for_col_name(name: str) -> str:
    """Return a dummy value inferred from the column or parameter *name*."""
    n = name.lower()
    if _BOOL_NAME_RE.search(n):
        return "TRUE"
    if _DATE_NAME_RE.search(n):
        return "NOW()"
    if _ID_NAME_RE.search(n):
        return "42"
    if _NUMERIC_NAME_RE.search(n):
        return "1.0"
    return "'placeholder'"


def _dummy_for_position(pos: int) -> str:
    """Return a position-based default dummy (cycles through int/text/date)."""
    _defaults = ["42", "'placeholder'", "NOW()"]
    return _defaults[(pos - 1) % len(_defaults)]


# ---------------------------------------------------------------------------
# Context resolution
# ---------------------------------------------------------------------------


def _resolve_dummy(
    pre_context: str,
    param_name: str | None,
    schema: SchemaInfo | None,
) -> str | None:
    """Return the best dummy value for a parameter, or *None* on failure.

    Resolution order:
    1. Column found in SQL context → schema type lookup → name heuristic.
    2. *param_name* (for named params) → schema type lookup → name heuristic.
    """
    col_name: str | None = None

    m = _BEFORE_PARAM_RE.search(pre_context)
    if m:
        col_name = m.group(1).split(".")[-1]  # strip table qualifier

    # Schema-based lookup
    if schema:
        for candidate in (c for c in [col_name, param_name] if c):
            for table_info in schema.tables.values():
                for column in table_info.columns:
                    if column.name.lower() == candidate.lower():
                        return _dummy_for_col_type(column.type)

    # Name-heuristic fallback
    for candidate in (c for c in [col_name, param_name] if c):
        return _dummy_for_col_name(candidate)

    return None


# ---------------------------------------------------------------------------
# Internal substitution helpers
# ---------------------------------------------------------------------------


def _sub_note(label: str, dummy: str, pre_context: str) -> str:
    """Build a substitution log entry, appending the column name when found."""
    col_m = _BEFORE_PARAM_RE.search(pre_context)
    if col_m:
        col = col_m.group(1).split(".")[-1]  # strip table qualifier
        return f"{label}={dummy} (column: {col})"
    return f"{label}={dummy}"


def _substitute_pg_positional(
    sql: str, schema: SchemaInfo | None
) -> tuple[str, list[str]]:
    substitutions: list[str] = []
    parts: list[str] = []
    last = 0

    for m in _POSITIONAL_PG.finditer(sql):
        pos = int(m.group(1))
        pre = sql[: m.start()]
        dummy = _resolve_dummy(pre, None, schema) or _dummy_for_position(pos)
        substitutions.append(_sub_note(f"${pos}", dummy, pre))

        parts.append(sql[last : m.start()])
        parts.append(dummy)
        last = m.end()

    parts.append(sql[last:])
    return "".join(parts), substitutions


def _substitute_psycopg(sql: str, schema: SchemaInfo | None) -> tuple[str, list[str]]:
    substitutions: list[str] = []
    parts: list[str] = []
    last = 0

    for pos, m in enumerate(_POSITIONAL_PSYCOPG.finditer(sql), 1):
        pre = sql[: m.start()]
        dummy = _resolve_dummy(pre, None, schema) or _dummy_for_position(pos)
        substitutions.append(_sub_note(f"%s[{pos}]", dummy, pre))

        parts.append(sql[last : m.start()])
        parts.append(dummy)
        last = m.end()

    parts.append(sql[last:])
    return "".join(parts), substitutions


def _substitute_named(sql: str, schema: SchemaInfo | None) -> tuple[str, list[str]]:
    substitutions: list[str] = []
    parts: list[str] = []
    last = 0

    for m in _NAMED_PARAM.finditer(sql):
        name = m.group(1)
        pre = sql[: m.start()]
        dummy = _resolve_dummy(pre, name, schema) or _dummy_for_col_name(name)
        substitutions.append(f":{name}={dummy}")

        parts.append(sql[last : m.start()])
        parts.append(dummy)
        last = m.end()

    parts.append(sql[last:])
    return "".join(parts), substitutions


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def substitute_params(
    sql: str,
    schema: SchemaInfo | None = None,
) -> tuple[str, list[str]]:
    """Substitute parameterized query placeholders with realistic dummy values.

    Supports:

    * ``$1``, ``$2``, … (PostgreSQL positional)
    * ``%s`` (psycopg2 positional)
    * ``:param_name`` (SQLAlchemy named)

    Args:
        sql: The SQL statement to process.
        schema: Optional schema information for type-aware substitution.

    Returns:
        A tuple of ``(substituted_sql, list_of_substitutions_made)``.
        Each entry in the substitution list documents a single replacement,
        e.g. ``"$1=42 (column: user_id)"``.  If no parameters are found the
        original SQL is returned together with an empty list.
    """
    if _POSITIONAL_PG.search(sql):
        return _substitute_pg_positional(sql, schema)
    if _POSITIONAL_PSYCOPG.search(sql):
        return _substitute_psycopg(sql, schema)
    if _NAMED_PARAM.search(sql):
        return _substitute_named(sql, schema)
    return sql, []


def make_notes(substitutions: list[str]) -> str | None:
    """Format *substitutions* into a human-readable notes string.

    Intended for use with :attr:`~pgreviewer.core.models.ExtractedQuery.notes`
    to document the dummy values that were injected before running EXPLAIN.

    Returns *None* when *substitutions* is empty.
    """
    if not substitutions:
        return None
    return "analyzed with dummy parameters: " + ", ".join(substitutions)
