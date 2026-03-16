"""Algorithmic index candidate generation.

Rules implemented
-----------------
1. Equality filter on a single column        → btree on that column
2. Equality on multiple columns              → composite btree, most selective first
3. Range filter (>, <, >=, <=, BETWEEN)      → btree on that column
4. High null_fraction column                 → partial index WHERE column IS NOT NULL
5. Equality literal + low most_common_freq   → partial index WHERE column = 'value'
6. Sort column (sort_without_index issues)   → covering btree index
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from pgreviewer.core.models import Issue, SchemaInfo

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

_NULL_FRACTION_THRESHOLD: float = 0.20
"""null_fraction above this triggers a partial IS NOT NULL index."""

_VALUE_FREQ_THRESHOLD: float = 0.20
"""If the minimum most_common_freq for a column is below this, a partial
index on the literal filter value is suggested."""

# ---------------------------------------------------------------------------
# SQL keywords that can never be column names
# ---------------------------------------------------------------------------

_SQL_KEYWORDS = frozenset(
    {
        "and",
        "or",
        "not",
        "is",
        "null",
        "true",
        "false",
        "in",
        "like",
        "ilike",
        "between",
        "any",
        "all",
        "exists",
        "case",
        "when",
        "then",
        "else",
        "end",
    }
)

# ---------------------------------------------------------------------------
# Compiled regexes
# ---------------------------------------------------------------------------

# Matches: identifier = 'string_literal' with optional ::cast
# Captures (column_name, literal_value)
_EQUALITY_LITERAL_RE = re.compile(
    r"\b([a-z_][a-z0-9_]*)\s*=\s*'([^']*)'(?:::[\w]+(?:\[\])?)*",
    re.IGNORECASE,
)

# Matches: identifier followed by a range comparison operator
# Captures column_name
_RANGE_COL_RE = re.compile(
    r"\b([a-z_][a-z0-9_]*)\s*(?:>=|<=|>|<|between\b)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


class IndexCandidate(BaseModel):
    """A suggested index to address a detected query-plan issue."""

    table: str
    columns: list[str]
    index_type: str = "btree"
    partial_predicate: str | None = None
    rationale: str


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _is_keyword(name: str) -> bool:
    return name.lower() in _SQL_KEYWORDS


def _null_fraction(table: str, col: str, schema: SchemaInfo) -> float:
    """Return ``null_fraction`` for *col* in *table*, defaulting to 0.0."""
    table_info = schema.tables.get(table)
    if not table_info:
        return 0.0
    for col_info in table_info.columns:
        if col_info.name == col:
            return col_info.null_fraction
    return 0.0


def _min_common_freq(table: str, col: str, schema: SchemaInfo) -> float | None:
    """Return the minimum value in ``most_common_freqs`` for *col*, or ``None``."""
    table_info = schema.tables.get(table)
    if not table_info:
        return None
    for col_info in table_info.columns:
        if col_info.name == col and col_info.most_common_freqs:
            return min(col_info.most_common_freqs)
    return None


def _column_selectivity(table: str, col: str, schema: SchemaInfo) -> float:
    """Estimate selectivity (fraction of rows matched by an equality predicate).

    Lower value → more selective → should lead a composite index.
    Uses ``distinct_count`` from :class:`ColumnInfo`:
    * Positive value: estimated number of distinct values → selectivity ≈ 1 / n
    * Negative value (PostgreSQL convention): ``-n_distinct`` is already a fraction
    """
    table_info = schema.tables.get(table)
    if not table_info:
        return 1.0
    for col_info in table_info.columns:
        if col_info.name != col:
            continue
        n = col_info.distinct_count
        if n > 0:
            return 1.0 / n
        if n < 0:
            # PostgreSQL n_distinct as negative fraction, e.g. -0.05 → selectivity=0.05
            return -n
    return 1.0


def _order_by_selectivity(
    table: str, columns: list[str], schema: SchemaInfo
) -> list[str]:
    """Return *columns* sorted most-selective-first (lowest fraction first)."""
    return sorted(columns, key=lambda c: _column_selectivity(table, c, schema))


def _parse_equality_literals(filter_expr: str) -> list[tuple[str, str]]:
    """Return ``(column, literal_value)`` pairs for equality predicates
    that compare against a non-empty string literal (not a placeholder like ``$1``)."""
    return [
        (col, val)
        for col, val in _EQUALITY_LITERAL_RE.findall(filter_expr)
        if not _is_keyword(col) and val  # skip empty string literals
    ]


def _parse_range_columns(filter_expr: str) -> list[str]:
    """Return column names that appear in range predicates."""
    return [col for col in _RANGE_COL_RE.findall(filter_expr) if not _is_keyword(col)]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def suggest_indexes(issues: list[Issue], schema: SchemaInfo) -> list[IndexCandidate]:
    """Produce index candidates by analysing *issues* against *schema* statistics.

    Parameters
    ----------
    issues:
        Issues produced by query-plan detectors.
    schema:
        Schema metadata including per-column statistics.

    Returns
    -------
    list[IndexCandidate]
        De-duplicated list of index candidates ordered by detection order.
    """
    candidates: list[IndexCandidate] = []
    seen: set[tuple] = set()

    def _add(candidate: IndexCandidate) -> None:
        key = (
            candidate.table,
            tuple(candidate.columns),
            candidate.partial_predicate,
        )
        if key not in seen:
            seen.add(key)
            candidates.append(candidate)

    for issue in issues:
        if not issue.affected_table:
            continue

        table = issue.affected_table
        filter_expr: str = issue.context.get("filter_expr", "") or ""

        # Deduplicate and strip SQL keywords from the column list.
        columns: list[str] = list(
            dict.fromkeys(c for c in issue.affected_columns if not _is_keyword(c))
        )

        # ------------------------------------------------------------------ #
        # Sort / covering-index rule                                           #
        # ------------------------------------------------------------------ #
        if issue.detector_name == "sort_without_index":
            if columns:
                ordered = _order_by_selectivity(table, columns, schema)
                _add(
                    IndexCandidate(
                        table=table,
                        columns=ordered,
                        index_type="btree",
                        rationale=(
                            f"Covering index on {table}({', '.join(ordered)}) "
                            "to avoid an explicit Sort step"
                        ),
                    )
                )
            continue

        if not columns:
            continue

        # ------------------------------------------------------------------ #
        # Null-fraction partial index                                          #
        # ------------------------------------------------------------------ #
        null_partial_cols: set[str] = set()
        for col in columns:
            nf = _null_fraction(table, col, schema)
            if nf > _NULL_FRACTION_THRESHOLD:
                null_partial_cols.add(col)
                _add(
                    IndexCandidate(
                        table=table,
                        columns=[col],
                        index_type="btree",
                        partial_predicate=f"{col} IS NOT NULL",
                        rationale=(
                            f"Partial index on {table}({col}) "
                            f"WHERE {col} IS NOT NULL; "
                            f"{nf:.0%} of rows are NULL so a full index "
                            "would waste space"
                        ),
                    )
                )

        # ------------------------------------------------------------------ #
        # Equality-literal partial index (low-frequency value)                #
        # ------------------------------------------------------------------ #
        lit_partial_cols: set[str] = set()
        if filter_expr:
            for col, val in _parse_equality_literals(filter_expr):
                if col not in columns:
                    continue
                min_freq = _min_common_freq(table, col, schema)
                if min_freq is not None and min_freq < _VALUE_FREQ_THRESHOLD:
                    lit_partial_cols.add(col)
                    pred = f"{col} = '{val}'"
                    _add(
                        IndexCandidate(
                            table=table,
                            columns=[col],
                            index_type="btree",
                            partial_predicate=pred,
                            rationale=(
                                f"Partial index on {table}({col}) "
                                f"WHERE {pred}; "
                                f"the value '{val}' is rare "
                                f"(min column freq {min_freq:.0%}), "
                                "so the partial index covers a small "
                                "fraction of the table"
                            ),
                        )
                    )

        # ------------------------------------------------------------------ #
        # Range-filter btree index                                             #
        # ------------------------------------------------------------------ #
        range_col_set: set[str] = set()
        if filter_expr:
            for col in _parse_range_columns(filter_expr):
                if col in columns:
                    range_col_set.add(col)
                    _add(
                        IndexCandidate(
                            table=table,
                            columns=[col],
                            index_type="btree",
                            rationale=(
                                f"Btree index on {table}({col}) to support range filter"
                            ),
                        )
                    )

        # ------------------------------------------------------------------ #
        # Equality btree index (single column or composite)                   #
        # ------------------------------------------------------------------ #
        # Exclude columns that already have a more targeted suggestion.
        eq_cols = [
            c
            for c in columns
            if c not in null_partial_cols
            and c not in lit_partial_cols
            and c not in range_col_set
        ]

        if len(eq_cols) == 1:
            col = eq_cols[0]
            _add(
                IndexCandidate(
                    table=table,
                    columns=[col],
                    index_type="btree",
                    rationale=f"Btree index on {table}({col}) for equality filter",
                )
            )
        elif len(eq_cols) > 1:
            ordered = _order_by_selectivity(table, eq_cols, schema)
            _add(
                IndexCandidate(
                    table=table,
                    columns=ordered,
                    index_type="btree",
                    rationale=(
                        f"Composite btree index on "
                        f"{table}({', '.join(ordered)}) "
                        "for multiple equality filters, "
                        "ordered by selectivity"
                    ),
                )
            )

    return candidates
