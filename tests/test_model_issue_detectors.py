"""Unit tests for pgreviewer.analysis.model_issue_detectors.

Covers:
- detect_missing_fk_index: FK without index → CRITICAL; with index=True → no issue;
  with explicit Index → no issue; with unique=True → no issue
- detect_removed_index: removed named index → WARNING; high-traffic table → CRITICAL
- detect_large_text_without_constraint: Text column → INFO; String(50) → no issue;
  bare String → INFO; other types → no issue
- detect_duplicate_pk_index: explicit index on PK column → WARNING; non-PK → no issue
- run_model_issue_detectors: convenience wrapper returns all issues
"""

from __future__ import annotations

from pgreviewer.analysis.model_issue_detectors import (
    detect_duplicate_pk_index,
    detect_large_text_without_constraint,
    detect_missing_fk_index,
    detect_removed_index,
    run_model_issue_detectors,
)
from pgreviewer.core.models import SchemaInfo, Severity, TableInfo
from pgreviewer.parsing.model_differ import ModelDiff
from pgreviewer.parsing.sqlalchemy_analyzer import ColumnDef, FKDef, IndexDef

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _diff(
    *,
    class_name: str = "Order",
    table_name: str = "orders",
    added_columns: list[ColumnDef] | None = None,
    removed_columns: list[ColumnDef] | None = None,
    added_indexes: list[IndexDef] | None = None,
    removed_indexes: list[IndexDef] | None = None,
    added_foreign_keys: list[FKDef] | None = None,
    removed_foreign_keys: list[FKDef] | None = None,
    pk_columns: list[str] | None = None,
) -> ModelDiff:
    return ModelDiff(
        class_name=class_name,
        table_name=table_name,
        added_columns=added_columns or [],
        removed_columns=removed_columns or [],
        added_indexes=added_indexes or [],
        removed_indexes=removed_indexes or [],
        added_foreign_keys=added_foreign_keys or [],
        removed_foreign_keys=removed_foreign_keys or [],
        pk_columns=pk_columns or [],
    )


def _fk(column_name: str, target: str = "users.id") -> FKDef:
    return FKDef(column_name=column_name, target=target)


def _col(
    name: str,
    col_type: str = "Integer",
    *,
    index: bool = False,
    unique: bool = False,
    primary_key: bool = False,
    has_type_args: bool = False,
) -> ColumnDef:
    return ColumnDef(
        name=name,
        col_type=col_type,
        index=index,
        unique=unique,
        primary_key=primary_key,
        has_type_args=has_type_args,
    )


def _idx(name: str | None, columns: list[str], is_unique: bool = False) -> IndexDef:
    return IndexDef(name=name, columns=columns, is_unique=is_unique)


# ---------------------------------------------------------------------------
# detect_missing_fk_index
# ---------------------------------------------------------------------------


class TestDetectMissingFkIndex:
    """FK column without index → CRITICAL; indexed variants → no issue."""

    def test_fk_without_index_is_warning_no_schema(self):
        """Without schema data (degraded-static mode), severity is WARNING."""
        diff = _diff(
            added_columns=[_col("user_id", "Integer")],
            added_foreign_keys=[_fk("user_id")],
        )
        issues = detect_missing_fk_index(diff)

        assert len(issues) == 1
        issue = issues[0]
        assert issue.severity == Severity.WARNING
        assert issue.detector_name == "missing_fk_index"
        assert "user_id" in issue.description
        assert issue.affected_table == "orders"
        assert issue.affected_columns == ["user_id"]
        assert issue.context["fk_target"] == "users.id"

    def test_fk_with_index_true_produces_no_issue(self):
        """FK column with ``index=True`` must not be flagged."""
        diff = _diff(
            added_columns=[_col("user_id", "Integer", index=True)],
            added_foreign_keys=[_fk("user_id")],
        )
        assert detect_missing_fk_index(diff) == []

    def test_fk_with_unique_true_produces_no_issue(self):
        """FK column with ``unique=True`` must not be flagged (unique implies index)."""
        diff = _diff(
            added_columns=[_col("user_id", "Integer", unique=True)],
            added_foreign_keys=[_fk("user_id")],
        )
        assert detect_missing_fk_index(diff) == []

    def test_fk_with_explicit_index_produces_no_issue(self):
        """FK column covered by an explicit ``Index(...)`` must not be flagged."""
        diff = _diff(
            added_columns=[_col("user_id", "Integer")],
            added_indexes=[_idx("ix_orders_user_id", ["user_id"])],
            added_foreign_keys=[_fk("user_id")],
        )
        assert detect_missing_fk_index(diff) == []

    def test_multiple_fks_flags_only_unindexed(self):
        """Only the FK column without an index is flagged when multiple FKs exist."""
        diff = _diff(
            added_columns=[
                _col("user_id", "Integer", index=True),
                _col("product_id", "Integer"),
            ],
            added_foreign_keys=[_fk("user_id"), _fk("product_id", "products.id")],
        )
        issues = detect_missing_fk_index(diff)

        assert len(issues) == 1
        assert issues[0].affected_columns == ["product_id"]

    def test_no_fks_produces_no_issue(self):
        """A diff with no FK columns must produce no issues."""
        diff = _diff(added_columns=[_col("name", "String")])
        assert detect_missing_fk_index(diff) == []

    def test_fk_covered_by_composite_index_produces_no_issue(self):
        """FK column that is part of a composite index must not be flagged."""
        diff = _diff(
            added_columns=[_col("user_id", "Integer")],
            added_indexes=[_idx("ix_orders_user_created", ["user_id", "created_at"])],
            added_foreign_keys=[_fk("user_id")],
        )
        assert detect_missing_fk_index(diff) == []


# ---------------------------------------------------------------------------
# detect_removed_index
# ---------------------------------------------------------------------------


class TestDetectRemovedIndex:
    """Removed named indexes are flagged; severity depends on table row count."""

    def test_removed_named_index_is_warning(self):
        """Removing a named index on a small/unknown table → WARNING."""
        diff = _diff(removed_indexes=[_idx("ix_orders_status", ["status"])])
        issues = detect_removed_index(diff, SchemaInfo())

        assert len(issues) == 1
        issue = issues[0]
        assert issue.severity == Severity.WARNING
        assert issue.detector_name == "removed_index"
        assert "ix_orders_status" in issue.description
        assert issue.affected_columns == ["status"]

    def test_removed_index_on_large_table_is_critical(self):
        """Removing an index on a table with >100K rows → CRITICAL."""
        schema = SchemaInfo(tables={"orders": TableInfo(row_estimate=500_000)})
        diff = _diff(removed_indexes=[_idx("ix_orders_status", ["status"])])
        issues = detect_removed_index(diff, schema)

        assert len(issues) == 1
        assert issues[0].severity == Severity.CRITICAL
        assert issues[0].context["row_estimate"] == 500_000

    def test_removed_index_on_boundary_table_is_warning(self):
        """Exactly 100K rows is NOT above the threshold → WARNING."""
        schema = SchemaInfo(tables={"orders": TableInfo(row_estimate=100_000)})
        diff = _diff(removed_indexes=[_idx("ix_orders_status", ["status"])])
        issues = detect_removed_index(diff, schema)

        assert issues[0].severity == Severity.WARNING

    def test_no_removed_indexes_produces_no_issue(self):
        """A diff with no removed indexes must produce no issues."""
        diff = _diff(added_columns=[_col("name", "String")])
        assert detect_removed_index(diff, SchemaInfo()) == []

    def test_unnamed_removed_index_still_flagged(self):
        """Unnamed (auto-generated) removed indexes are also flagged."""
        diff = _diff(removed_indexes=[_idx(None, ["status"])])
        issues = detect_removed_index(diff, SchemaInfo())

        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING

    def test_removed_index_context_contains_index_name(self):
        """The context dict must include the index name for diagnostics."""
        diff = _diff(removed_indexes=[_idx("ix_orders_ref", ["ref"])])
        issues = detect_removed_index(diff, SchemaInfo())

        assert issues[0].context["index_name"] == "ix_orders_ref"


# ---------------------------------------------------------------------------
# detect_large_text_without_constraint
# ---------------------------------------------------------------------------


class TestDetectLargeTextWithoutConstraint:
    """Unconstrained text columns → INFO; constrained or other types → no issue."""

    def test_text_column_is_info(self):
        """A new ``Text`` column must produce an INFO issue."""
        diff = _diff(added_columns=[_col("notes", "Text")])
        issues = detect_large_text_without_constraint(diff)

        assert len(issues) == 1
        issue = issues[0]
        assert issue.severity == Severity.INFO
        assert issue.detector_name == "large_text_without_constraint"
        assert "notes" in issue.description

    def test_unconstrained_string_is_info(self):
        """A new ``String`` column without length → INFO."""
        diff = _diff(added_columns=[_col("label", "String", has_type_args=False)])
        issues = detect_large_text_without_constraint(diff)

        assert len(issues) == 1
        assert issues[0].severity == Severity.INFO

    def test_constrained_string_produces_no_issue(self):
        """``String(255)`` (has_type_args=True) must not be flagged."""
        diff = _diff(added_columns=[_col("label", "String", has_type_args=True)])
        assert detect_large_text_without_constraint(diff) == []

    def test_integer_column_produces_no_issue(self):
        """Non-text column types must not produce any issues."""
        diff = _diff(added_columns=[_col("count", "Integer")])
        assert detect_large_text_without_constraint(diff) == []

    def test_unicode_text_is_flagged(self):
        """``UnicodeText`` is always unconstrained and must be flagged."""
        diff = _diff(added_columns=[_col("bio", "UnicodeText")])
        issues = detect_large_text_without_constraint(diff)

        assert len(issues) == 1
        assert issues[0].severity == Severity.INFO

    def test_multiple_text_columns_each_flagged(self):
        """Each unconstrained text column gets its own issue."""
        diff = _diff(
            added_columns=[_col("a", "Text"), _col("b", "Text"), _col("c", "Integer")]
        )
        issues = detect_large_text_without_constraint(diff)

        assert len(issues) == 2
        flagged_cols = {i.affected_columns[0] for i in issues}
        assert flagged_cols == {"a", "b"}


# ---------------------------------------------------------------------------
# detect_duplicate_pk_index
# ---------------------------------------------------------------------------


class TestDetectDuplicatePkIndex:
    """Explicit indexes on PK columns → WARNING; non-PK indexes → no issue."""

    def test_index_on_pk_column_is_warning(self):
        """An explicit index that covers only a PK column → WARNING."""
        diff = _diff(
            pk_columns=["id"],
            added_indexes=[_idx("ix_orders_id", ["id"])],
        )
        issues = detect_duplicate_pk_index(diff)

        assert len(issues) == 1
        issue = issues[0]
        assert issue.severity == Severity.WARNING
        assert issue.detector_name == "duplicate_pk_index"
        assert "id" in issue.description
        assert issue.affected_columns == ["id"]

    def test_index_on_non_pk_column_produces_no_issue(self):
        """An explicit index on a non-PK column must not be flagged."""
        diff = _diff(
            pk_columns=["id"],
            added_indexes=[_idx("ix_orders_status", ["status"])],
        )
        assert detect_duplicate_pk_index(diff) == []

    def test_composite_index_all_pk_is_warning(self):
        """A composite index where all columns are PKs → WARNING."""
        diff = _diff(
            pk_columns=["tenant_id", "order_id"],
            added_indexes=[_idx("ix_composite_pk", ["tenant_id", "order_id"])],
        )
        issues = detect_duplicate_pk_index(diff)

        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING

    def test_composite_index_mixed_pk_non_pk_produces_no_issue(self):
        """A composite index with some non-PK columns must not be flagged."""
        diff = _diff(
            pk_columns=["id"],
            added_indexes=[_idx("ix_id_status", ["id", "status"])],
        )
        assert detect_duplicate_pk_index(diff) == []

    def test_no_pk_columns_produces_no_issue(self):
        """When pk_columns is empty, no duplicate-PK check is possible."""
        diff = _diff(
            pk_columns=[],
            added_indexes=[_idx("ix_orders_id", ["id"])],
        )
        assert detect_duplicate_pk_index(diff) == []

    def test_unnamed_index_on_pk_is_warning(self):
        """An unnamed index covering only a PK column → WARNING."""
        diff = _diff(
            pk_columns=["id"],
            added_indexes=[_idx(None, ["id"])],
        )
        issues = detect_duplicate_pk_index(diff)

        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING


# ---------------------------------------------------------------------------
# run_model_issue_detectors
# ---------------------------------------------------------------------------


class TestRunModelIssueDetectors:
    """Convenience wrapper runs all detectors and aggregates results."""

    def test_all_detectors_run(self):
        """A diff that triggers all four detectors returns issues from each."""
        diff = _diff(
            table_name="orders",
            # FK without index → missing_fk_index
            added_columns=[
                _col("user_id", "Integer"),
                _col("notes", "Text"),
            ],
            added_foreign_keys=[_fk("user_id")],
            # Removed index → removed_index
            removed_indexes=[_idx("ix_orders_ref", ["ref"])],
            # Duplicate PK index → duplicate_pk_index
            pk_columns=["id"],
            added_indexes=[_idx("ix_orders_id", ["id"])],
        )
        issues = run_model_issue_detectors(diff)

        detector_names = {i.detector_name for i in issues}
        assert "missing_fk_index" in detector_names
        assert "removed_index" in detector_names
        assert "large_text_without_constraint" in detector_names
        assert "duplicate_pk_index" in detector_names

    def test_empty_diff_produces_no_issues(self):
        """A diff with no structural changes produces no issues."""
        diff = _diff()
        assert run_model_issue_detectors(diff) == []

    def test_none_schema_uses_empty_schema(self):
        """Passing ``schema=None`` must not raise and uses an empty SchemaInfo."""
        diff = _diff(removed_indexes=[_idx("ix_orders_status", ["status"])])
        issues = run_model_issue_detectors(diff, schema=None)

        # Should still flag the removed index at WARNING level (no row estimate)
        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING

    def test_fk_with_index_produces_no_missing_fk_issue(self):
        """The 'done-when' acceptance criterion from the issue spec."""
        # Without index=True → WARNING (degraded-static mode, no schema)
        diff_without = _diff(
            added_columns=[_col("user_id", "Integer")],
            added_foreign_keys=[_fk("user_id", "users.id")],
        )
        issues_without = run_model_issue_detectors(diff_without)
        fk_issues = [i for i in issues_without if i.detector_name == "missing_fk_index"]
        assert len(fk_issues) == 1
        assert fk_issues[0].severity == Severity.WARNING
        assert "user_id" in fk_issues[0].description

        # With index=True → no missing_fk_index issue
        diff_with = _diff(
            added_columns=[_col("user_id", "Integer", index=True)],
            added_foreign_keys=[_fk("user_id", "users.id")],
        )
        issues_with = run_model_issue_detectors(diff_with)
        fk_issues_with = [
            i for i in issues_with if i.detector_name == "missing_fk_index"
        ]
        assert fk_issues_with == []
