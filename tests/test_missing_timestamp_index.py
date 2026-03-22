"""Tests for MissingTimestampIndexDetector."""

from __future__ import annotations

from pgreviewer.analysis.migration_detectors import (
    parse_ddl_statement,
    run_migration_detectors,
)
from pgreviewer.core.models import (
    IndexInfo,
    ParsedMigration,
    SchemaInfo,
    Severity,
    TableInfo,
)


def _migration(*sqls: str, source_file: str = "migrations/0001.sql") -> ParsedMigration:
    return ParsedMigration(
        statements=[parse_ddl_statement(sql, i + 1) for i, sql in enumerate(sqls)],
        source_file=source_file,
    )


def _issues(migration: ParsedMigration, schema: SchemaInfo) -> list:
    return [
        i
        for i in run_migration_detectors(migration, schema)
        if i.detector_name == "missing_timestamp_index"
    ]


# ---------------------------------------------------------------------------
# Degraded-static (no schema)
# ---------------------------------------------------------------------------


class TestDegradedStatic:
    def test_add_timestamp_column_warns_no_schema(self):
        m = _migration("ALTER TABLE events ADD COLUMN created_at timestamp;")
        issues = _issues(m, SchemaInfo())
        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING
        assert "created_at" in issues[0].affected_columns
        assert "schema data" in issues[0].description

    def test_add_timestamptz_column_warns(self):
        m = _migration("ALTER TABLE events ADD COLUMN updated_at timestamptz NOT NULL;")
        issues = _issues(m, SchemaInfo())
        assert len(issues) == 1
        assert issues[0].affected_columns == ["updated_at"]

    def test_add_timestamp_with_time_zone_warns(self):
        m = _migration("ALTER TABLE t ADD COLUMN ts timestamp with time zone;")
        issues = _issues(m, SchemaInfo())
        assert len(issues) == 1

    def test_non_timestamp_column_ignored(self):
        m = _migration("ALTER TABLE t ADD COLUMN name text;")
        issues = _issues(m, SchemaInfo())
        assert len(issues) == 0

    def test_integer_column_ignored(self):
        m = _migration("ALTER TABLE t ADD COLUMN count integer;")
        issues = _issues(m, SchemaInfo())
        assert len(issues) == 0

    def test_index_in_same_migration_suppresses(self):
        m = _migration(
            "ALTER TABLE events ADD COLUMN created_at timestamp;",
            "CREATE INDEX CONCURRENTLY idx_events_created ON events (created_at);",
        )
        issues = _issues(m, SchemaInfo())
        assert len(issues) == 0


# ---------------------------------------------------------------------------
# With schema (full precision)
# ---------------------------------------------------------------------------


class TestWithSchema:
    def test_small_table_stays_warning(self):
        schema = SchemaInfo(tables={"events": TableInfo(row_estimate=10_000)})
        m = _migration("ALTER TABLE events ADD COLUMN created_at timestamp;")
        issues = _issues(m, schema)
        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING

    def test_large_table_escalates_to_critical(self):
        schema = SchemaInfo(tables={"events": TableInfo(row_estimate=500_000)})
        m = _migration("ALTER TABLE events ADD COLUMN created_at timestamp;")
        issues = _issues(m, schema)
        assert len(issues) == 1
        assert issues[0].severity == Severity.CRITICAL

    def test_existing_schema_index_suppresses(self):
        schema = SchemaInfo(
            tables={
                "events": TableInfo(
                    row_estimate=500_000,
                    indexes=[IndexInfo(name="idx_ts", columns=["created_at"])],
                )
            }
        )
        m = _migration("ALTER TABLE events ADD COLUMN created_at timestamp;")
        issues = _issues(m, schema)
        assert len(issues) == 0

    def test_new_table_in_pr_stays_warning(self):
        """Table not in schema (created in same PR) → WARNING, not CRITICAL."""
        schema = SchemaInfo(tables={"other_table": TableInfo(row_estimate=999_999)})
        m = _migration("ALTER TABLE new_table ADD COLUMN created_at timestamp;")
        issues = _issues(m, schema)
        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING

    def test_row_estimate_in_description(self):
        schema = SchemaInfo(tables={"events": TableInfo(row_estimate=42_000)})
        m = _migration("ALTER TABLE events ADD COLUMN created_at timestamp;")
        issues = _issues(m, schema)
        assert "42000" in issues[0].description

    def test_suggested_action_has_concurrent_index(self):
        m = _migration("ALTER TABLE events ADD COLUMN created_at timestamp;")
        issues = _issues(m, SchemaInfo())
        assert "CREATE INDEX CONCURRENTLY" in issues[0].suggested_action
        assert "created_at" in issues[0].suggested_action


# ---------------------------------------------------------------------------
# CREATE TABLE (currently not detected — out of scope for ALTER TABLE detector)
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_if_not_exists_column_still_detected(self):
        m = _migration("ALTER TABLE t ADD COLUMN IF NOT EXISTS ts timestamptz;")
        issues = _issues(m, SchemaInfo())
        assert len(issues) == 1

    def test_multiple_timestamp_columns(self):
        m = _migration(
            "ALTER TABLE t ADD COLUMN created_at timestamp;",
            "ALTER TABLE t ADD COLUMN updated_at timestamptz;",
        )
        issues = _issues(m, SchemaInfo())
        assert len(issues) == 2

    def test_no_false_positive_on_non_alter(self):
        m = _migration("CREATE INDEX CONCURRENTLY idx ON t (ts);")
        issues = _issues(m, SchemaInfo())
        assert len(issues) == 0
