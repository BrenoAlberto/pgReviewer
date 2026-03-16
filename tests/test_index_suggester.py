"""Tests for pgreviewer.analysis.index_suggester."""

from pgreviewer.analysis.index_suggester import IndexCandidate, suggest_indexes
from pgreviewer.core.models import (
    ColumnInfo,
    Issue,
    SchemaInfo,
    Severity,
    TableInfo,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _missing_index_issue(
    table: str,
    columns: list[str],
    filter_expr: str = "",
) -> Issue:
    return Issue(
        detector_name="missing_index_on_filter",
        severity=Severity.WARNING,
        description="Seq Scan without covering index",
        affected_table=table,
        affected_columns=columns,
        suggested_action="Add an index",
        context={"filter_expr": filter_expr} if filter_expr else {},
    )


def _sort_issue(table: str, columns: list[str]) -> Issue:
    return Issue(
        detector_name="sort_without_index",
        severity=Severity.WARNING,
        description="Sort without index",
        affected_table=table,
        affected_columns=columns,
        suggested_action="Add an index",
        context={"input_rows": 5000},
    )


# ---------------------------------------------------------------------------
# "Done when" criteria
# ---------------------------------------------------------------------------


def test_btree_for_equality_filter_no_schema():
    """Seq scan with WHERE user_id = $1 must produce a btree candidate."""
    issue = _missing_index_issue(
        table="orders",
        columns=["user_id"],
        filter_expr="(user_id = $1)",
    )
    candidates = suggest_indexes([issue], SchemaInfo())

    assert len(candidates) == 1
    c = candidates[0]
    assert c.table == "orders"
    assert c.columns == ["user_id"]
    assert c.index_type == "btree"
    assert c.partial_predicate is None


def test_partial_index_for_low_frequency_value():
    """WHERE status = 'active' on a column that is active only 5% of the time
    must produce a partial-index candidate with the literal predicate."""
    schema = SchemaInfo(
        tables={
            "orders": TableInfo(
                row_estimate=100_000,
                columns=[
                    ColumnInfo(
                        name="status",
                        type="text",
                        null_fraction=0.0,
                        distinct_count=2,
                        most_common_freqs=[0.95, 0.05],
                    )
                ],
            )
        }
    )
    issue = _missing_index_issue(
        table="orders",
        columns=["status"],
        filter_expr="(status = 'active'::text)",
    )
    candidates = suggest_indexes([issue], schema)

    partial = [c for c in candidates if c.partial_predicate is not None]
    assert partial, "Expected at least one partial-index candidate"
    c = partial[0]
    assert c.table == "orders"
    assert c.columns == ["status"]
    assert c.index_type == "btree"
    assert "status" in c.partial_predicate
    assert "active" in c.partial_predicate


# ---------------------------------------------------------------------------
# Rule: null-fraction partial index
# ---------------------------------------------------------------------------


def test_partial_index_for_high_null_fraction():
    """Column with null_fraction > 0.20 must produce a partial IS NOT NULL index."""
    schema = SchemaInfo(
        tables={
            "users": TableInfo(
                columns=[
                    ColumnInfo(
                        name="deleted_at",
                        type="timestamp",
                        null_fraction=0.90,
                    )
                ]
            )
        }
    )
    issue = _missing_index_issue(
        table="users",
        columns=["deleted_at"],
        filter_expr="(deleted_at IS NOT NULL)",
    )
    candidates = suggest_indexes([issue], schema)

    assert any(c.partial_predicate == "deleted_at IS NOT NULL" for c in candidates), (
        "Expected a partial IS NOT NULL candidate"
    )


def test_no_partial_index_when_null_fraction_below_threshold():
    """Column with null_fraction below 0.20 must not get a partial IS NOT NULL."""
    schema = SchemaInfo(
        tables={
            "users": TableInfo(
                columns=[
                    ColumnInfo(
                        name="email",
                        type="text",
                        null_fraction=0.05,
                    )
                ]
            )
        }
    )
    issue = _missing_index_issue(
        table="users",
        columns=["email"],
        filter_expr="(email = 'test@example.com'::text)",
    )
    candidates = suggest_indexes([issue], schema)

    assert not any(
        c.partial_predicate and "IS NOT NULL" in c.partial_predicate for c in candidates
    )


# ---------------------------------------------------------------------------
# Rule: composite index, most selective first
# ---------------------------------------------------------------------------


def test_composite_index_ordered_by_selectivity():
    """Multiple equality columns must produce a composite index with
    the highest-cardinality column first."""
    schema = SchemaInfo(
        tables={
            "orders": TableInfo(
                columns=[
                    ColumnInfo(name="user_id", type="int", distinct_count=50_000),
                    ColumnInfo(name="status", type="text", distinct_count=3),
                ]
            )
        }
    )
    issue = _missing_index_issue(
        table="orders",
        columns=["user_id", "status"],
        filter_expr="((user_id = $1) AND (status = $2))",
    )
    candidates = suggest_indexes([issue], schema)

    composite = [c for c in candidates if len(c.columns) > 1]
    assert composite, "Expected a composite index candidate"
    c = composite[0]
    # user_id has higher cardinality → more selective → should come first
    assert c.columns[0] == "user_id"
    assert "status" in c.columns


def test_composite_index_no_schema_stats():
    """When no column stats are available, composite index preserves input order."""
    issue = _missing_index_issue(
        table="orders",
        columns=["user_id", "status"],
        filter_expr="((user_id = $1) AND (status = $2))",
    )
    candidates = suggest_indexes([issue], SchemaInfo())

    composite = [c for c in candidates if len(c.columns) > 1]
    assert composite
    assert set(composite[0].columns) == {"user_id", "status"}


# ---------------------------------------------------------------------------
# Rule: range filter → btree
# ---------------------------------------------------------------------------


def test_btree_for_range_filter_gt():
    """WHERE amount > $1 must produce a btree candidate for the range column."""
    issue = _missing_index_issue(
        table="orders",
        columns=["amount"],
        filter_expr="(amount > $1)",
    )
    candidates = suggest_indexes([issue], SchemaInfo())

    assert len(candidates) == 1
    c = candidates[0]
    assert c.columns == ["amount"]
    assert c.index_type == "btree"
    assert c.partial_predicate is None
    assert "range" in c.rationale.lower()


def test_btree_for_range_filter_between():
    """WHERE created_at BETWEEN $1 AND $2 must produce a btree candidate."""
    issue = _missing_index_issue(
        table="events",
        columns=["created_at"],
        filter_expr="(created_at BETWEEN $1 AND $2)",
    )
    candidates = suggest_indexes([issue], SchemaInfo())

    assert any(
        c.columns == ["created_at"] and c.partial_predicate is None for c in candidates
    )


# ---------------------------------------------------------------------------
# Rule: sort_without_index → covering index
# ---------------------------------------------------------------------------


def test_covering_index_for_sort():
    """sort_without_index issue must produce a btree covering index."""
    issue = _sort_issue(table="orders", columns=["created_at"])
    candidates = suggest_indexes([issue], SchemaInfo())

    assert len(candidates) == 1
    c = candidates[0]
    assert c.table == "orders"
    assert c.columns == ["created_at"]
    assert c.index_type == "btree"
    assert c.partial_predicate is None
    assert "Sort" in c.rationale


def test_covering_index_sort_with_selectivity_ordering():
    """Sort on multiple columns uses selectivity ordering."""
    schema = SchemaInfo(
        tables={
            "orders": TableInfo(
                columns=[
                    ColumnInfo(name="status", type="text", distinct_count=3),
                    ColumnInfo(
                        name="created_at",
                        type="timestamp",
                        distinct_count=90_000,
                    ),
                ]
            )
        }
    )
    issue = _sort_issue(table="orders", columns=["status", "created_at"])
    candidates = suggest_indexes([issue], schema)

    assert len(candidates) == 1
    c = candidates[0]
    # created_at has higher cardinality → more selective → should come first
    assert c.columns[0] == "created_at"


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


def test_deduplication_same_issue_twice():
    """Identical issues must produce only one candidate."""
    issue = _missing_index_issue(
        table="orders",
        columns=["user_id"],
        filter_expr="(user_id = $1)",
    )
    candidates = suggest_indexes([issue, issue], SchemaInfo())

    assert len(candidates) == 1


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_issues():
    """Empty issue list must return empty candidate list."""
    assert suggest_indexes([], SchemaInfo()) == []


def test_issue_without_affected_table():
    """Issue without affected_table must be silently skipped."""
    issue = Issue(
        detector_name="missing_index_on_filter",
        severity=Severity.WARNING,
        description="no table",
        affected_table=None,
        affected_columns=["col"],
        suggested_action="",
    )
    assert suggest_indexes([issue], SchemaInfo()) == []


def test_issue_without_affected_columns():
    """Issue without affected_columns (e.g. sequential_scan) must be skipped."""
    issue = Issue(
        detector_name="sequential_scan",
        severity=Severity.WARNING,
        description="seq scan",
        affected_table="orders",
        affected_columns=[],
        suggested_action="",
        context={"estimated_rows": 100_000},
    )
    assert suggest_indexes([issue], SchemaInfo()) == []


def test_sql_keywords_not_treated_as_columns():
    """SQL keywords appearing in filter expressions must not become column names,
    and the real column must still appear in the candidates."""
    issue = _missing_index_issue(
        table="users",
        columns=["deleted_at"],
        filter_expr="(deleted_at IS NOT NULL)",
    )
    candidates = suggest_indexes([issue], SchemaInfo())
    for c in candidates:
        for col in c.columns:
            assert col.lower() not in ("is", "not", "null", "and", "or")
    # The real column must be present in the suggestion
    all_cols = [col for c in candidates for col in c.columns]
    assert "deleted_at" in all_cols


def test_index_candidate_is_pydantic_model():
    """IndexCandidate must be a Pydantic model with the required fields."""
    c = IndexCandidate(
        table="orders",
        columns=["user_id"],
        index_type="btree",
        partial_predicate=None,
        rationale="test",
    )
    assert c.table == "orders"
    assert c.columns == ["user_id"]
    assert c.index_type == "btree"
    assert c.partial_predicate is None
    assert c.rationale == "test"
