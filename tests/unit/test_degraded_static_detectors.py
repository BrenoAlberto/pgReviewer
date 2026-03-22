"""Tests for degraded-static mode across the three schema-aware detectors.

Detectors under test
--------------------
1. FKWithoutIndexDetector  (migration_detectors/fk_without_index.py)
2. detect_missing_fk_index (model_issue_detectors.py)
3. detect_removed_index    (model_issue_detectors.py) – already correct, regression
   guard

Each detector must:
- Emit WARNING when schema is empty/unavailable (degraded-static mode)
- Emit CRITICAL when schema is available and confirms the issue
- Include a note about degraded confidence in the description when schema is absent
"""

from __future__ import annotations

from pgreviewer.analysis.migration_detectors.fk_without_index import (
    FKWithoutIndexDetector,
)
from pgreviewer.analysis.model_issue_detectors import (
    detect_missing_fk_index,
    detect_removed_index,
)
from pgreviewer.core.models import (
    DDLStatement,
    IndexInfo,
    ParsedMigration,
    SchemaInfo,
    Severity,
    TableInfo,
)
from pgreviewer.parsing.model_differ import ModelDiff
from pgreviewer.parsing.sqlalchemy_analyzer import ColumnDef, FKDef, IndexDef

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fk_migration(sql: str | None = None) -> ParsedMigration:
    raw = sql or (
        "ALTER TABLE orders ADD CONSTRAINT fk_user "
        "FOREIGN KEY (user_id) REFERENCES users(id)"
    )
    stmt = DDLStatement(
        statement_type="ALTER TABLE",
        table="orders",
        raw_sql=raw,
        line_number=1,
    )
    return ParsedMigration(statements=[stmt], source_file="migrations/0001.sql")


def _fk_diff(table: str = "orders") -> ModelDiff:
    fk = FKDef(column_name="user_id", target="users.id", line=10)
    col = ColumnDef(name="user_id", col_type="Integer", line=10)
    return ModelDiff(
        class_name="Order",
        table_name=table,
        added_columns=[col],
        added_foreign_keys=[fk],
    )


def _removed_index_diff(table: str = "orders") -> ModelDiff:
    idx = IndexDef(name="idx_orders_status", columns=["status"], line=None)
    return ModelDiff(
        class_name="Order",
        table_name=table,
        removed_indexes=[idx],
    )


def _schema_with_table(table: str, row_estimate: int = 100_000) -> SchemaInfo:
    schema = SchemaInfo()
    schema.tables[table] = TableInfo(row_estimate=row_estimate)
    return schema


def _schema_with_index(table: str, index_cols: list[str]) -> SchemaInfo:
    schema = SchemaInfo()
    schema.tables[table] = TableInfo(
        row_estimate=50_000,
        indexes=[IndexInfo(name="idx_test", columns=index_cols)],
    )
    return schema


# ---------------------------------------------------------------------------
# FKWithoutIndexDetector
# ---------------------------------------------------------------------------


class TestFKWithoutIndexDetectorDegradedMode:
    detector = FKWithoutIndexDetector()

    def test_degraded_emits_warning_no_schema(self) -> None:
        migration = _fk_migration()
        issues = self.detector.detect(migration, SchemaInfo())

        assert issues, "Expected FK-without-index finding"
        assert all(i.severity == Severity.WARNING for i in issues), (
            f"Expected WARNING in degraded mode, got {[i.severity for i in issues]}"
        )

    def test_degraded_description_mentions_schema(self) -> None:
        migration = _fk_migration()
        issues = self.detector.detect(migration, SchemaInfo())

        assert issues
        assert "schema data" in issues[0].description.lower(), (
            "Degraded-mode description should mention schema data"
        )

    def test_schema_aware_emits_critical(self) -> None:
        migration = _fk_migration()
        # Table must exceed CONCURRENT_INDEX_THRESHOLD (100k) to get CRITICAL
        schema = _schema_with_table("orders", row_estimate=500_000)
        issues = self.detector.detect(migration, schema)

        assert issues, "Expected FK-without-index finding with schema"
        assert all(i.severity == Severity.CRITICAL for i in issues), (
            f"Expected CRITICAL with schema, got {[i.severity for i in issues]}"
        )

    def test_schema_aware_suppressed_when_index_exists(self) -> None:
        migration = _fk_migration()
        schema = _schema_with_index("orders", ["user_id"])
        issues = self.detector.detect(migration, schema)

        fk_issues = [
            i for i in issues if i.detector_name == "add_foreign_key_without_index"
        ]
        assert not fk_issues, "Should suppress FK finding when index already exists"

    def test_same_migration_index_suppresses_in_degraded_mode(self) -> None:
        """Index in same migration suppresses FK finding regardless of mode."""
        fk_stmt = DDLStatement(
            statement_type="ALTER TABLE",
            table="orders",
            raw_sql=(
                "ALTER TABLE orders ADD CONSTRAINT fk_user "
                "FOREIGN KEY (user_id) REFERENCES users(id)"
            ),
            line_number=1,
        )
        idx_stmt = DDLStatement(
            statement_type="CREATE INDEX",
            table="orders",
            raw_sql="CREATE INDEX idx_orders_user_id ON orders (user_id)",
            line_number=2,
        )
        migration = ParsedMigration(
            statements=[fk_stmt, idx_stmt],
            source_file="migrations/0001.sql",
        )
        issues = self.detector.detect(migration, SchemaInfo())

        fk_issues = [
            i for i in issues if i.detector_name == "add_foreign_key_without_index"
        ]
        assert not fk_issues, (
            "In-migration CREATE INDEX should suppress FK finding even in degraded mode"
        )


# ---------------------------------------------------------------------------
# detect_missing_fk_index (model_issue_detectors)
# ---------------------------------------------------------------------------


class TestDetectMissingFKIndexDegradedMode:
    def test_degraded_emits_warning_no_schema(self) -> None:
        diff = _fk_diff()
        issues = detect_missing_fk_index(diff, schema=None)

        assert issues, "Expected FK-index finding"
        assert all(i.severity == Severity.WARNING for i in issues), (
            f"Expected WARNING in degraded mode, got {[i.severity for i in issues]}"
        )

    def test_degraded_emits_warning_empty_schema(self) -> None:
        diff = _fk_diff()
        issues = detect_missing_fk_index(diff, schema=SchemaInfo())

        assert issues
        assert all(i.severity == Severity.WARNING for i in issues)

    def test_degraded_description_mentions_schema(self) -> None:
        diff = _fk_diff()
        issues = detect_missing_fk_index(diff, schema=None)

        assert issues
        assert "schema data" in issues[0].description.lower()

    def test_schema_aware_emits_critical_for_large_table(self) -> None:
        diff = _fk_diff(table="orders")
        schema = _schema_with_table("orders", row_estimate=200_000)
        issues = detect_missing_fk_index(diff, schema=schema)

        assert issues
        assert all(i.severity == Severity.CRITICAL for i in issues), (
            f"Expected CRITICAL for large table with schema, "
            f"got {[i.severity for i in issues]}"
        )

    def test_schema_aware_emits_warning_for_empty_table(self) -> None:
        diff = _fk_diff(table="orders")
        schema = _schema_with_table("orders", row_estimate=0)
        issues = detect_missing_fk_index(diff, schema=schema)

        assert issues
        assert all(i.severity == Severity.WARNING for i in issues)

    def test_indexed_column_suppressed(self) -> None:
        fk = FKDef(column_name="user_id", target="users.id", line=10)
        col = ColumnDef(name="user_id", col_type="Integer", index=True, line=10)
        diff = ModelDiff(
            class_name="Order",
            table_name="orders",
            added_columns=[col],
            added_foreign_keys=[fk],
        )
        issues = detect_missing_fk_index(diff, schema=None)
        assert not issues, "Indexed FK column should not produce a finding"


# ---------------------------------------------------------------------------
# detect_removed_index – regression guard (already schema-aware)
# ---------------------------------------------------------------------------


class TestDetectRemovedIndexAlreadyDegraded:
    def test_no_schema_emits_warning(self) -> None:
        diff = _removed_index_diff()
        issues = detect_removed_index(diff, SchemaInfo())

        assert issues
        assert all(i.severity == Severity.WARNING for i in issues), (
            f"Expected WARNING without schema, got {[i.severity for i in issues]}"
        )

    def test_large_table_schema_emits_critical(self) -> None:
        diff = _removed_index_diff()
        schema = _schema_with_table("orders", row_estimate=200_000)
        issues = detect_removed_index(diff, schema)

        assert issues
        assert all(i.severity == Severity.CRITICAL for i in issues), (
            f"Expected CRITICAL for large table, got {[i.severity for i in issues]}"
        )

    def test_small_table_schema_emits_warning(self) -> None:
        diff = _removed_index_diff()
        schema = _schema_with_table("orders", row_estimate=1_000)
        issues = detect_removed_index(diff, schema)

        assert issues
        assert all(i.severity == Severity.WARNING for i in issues)
