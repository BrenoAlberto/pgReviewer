from pgreviewer.analysis.migration_detectors import (
    parse_ddl_statement,
    run_migration_detectors,
)
from pgreviewer.config import settings
from pgreviewer.core.models import ParsedMigration, SchemaInfo, Severity, TableInfo


def test_add_not_null_without_default_flags_critical_on_large_table(monkeypatch):
    monkeypatch.setattr(settings, "SEQ_SCAN_ROW_THRESHOLD", 10_000)

    # 500K rows as per prompt
    schema = SchemaInfo(tables={"large_table": TableInfo(row_estimate=500_000)})

    parsed = ParsedMigration(
        statements=[
            parse_ddl_statement(
                "ALTER TABLE large_table ADD COLUMN active BOOLEAN NOT NULL;", 5
            )
        ],
        source_file="migrations/0006_add_not_null.sql",
    )

    issues = run_migration_detectors(parsed, schema)
    detector_issues = [
        i for i in issues if i.detector_name == "add_not_null_without_default"
    ]

    assert len(detector_issues) == 1
    assert detector_issues[0].severity == Severity.CRITICAL
    assert "large_table" in detector_issues[0].affected_table
    assert "active" in detector_issues[0].affected_columns
    assert "two-phase approach" in detector_issues[0].suggested_action.lower()


def test_add_not_null_with_default_is_ignored():
    schema = SchemaInfo(tables={"large_table": TableInfo(row_estimate=500_000)})

    parsed = ParsedMigration(
        statements=[
            parse_ddl_statement(
                "ALTER TABLE large_table "
                "ADD COLUMN active BOOLEAN NOT NULL DEFAULT false;",
                7,
            )
        ],
        source_file="migrations/0007_add_not_null_default.sql",
    )

    issues = run_migration_detectors(parsed, schema)
    detector_issues = [
        i for i in issues if i.detector_name == "add_not_null_without_default"
    ]

    assert len(detector_issues) == 0


def test_alter_column_set_not_null_is_flagged():
    schema = SchemaInfo(tables={"large_table": TableInfo(row_estimate=500_000)})

    parsed = ParsedMigration(
        statements=[
            parse_ddl_statement(
                "ALTER TABLE large_table ALTER COLUMN name SET NOT NULL;", 10
            )
        ],
        source_file="migrations/0008_set_not_null.sql",
    )

    issues = run_migration_detectors(parsed, schema)
    detector_issues = [
        i for i in issues if i.detector_name == "add_not_null_without_default"
    ]

    assert len(detector_issues) == 1
    assert detector_issues[0].severity == Severity.CRITICAL
    assert "name" in detector_issues[0].affected_columns
    assert "CHECK constraint" in detector_issues[0].suggested_action
