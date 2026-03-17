from pgreviewer.analysis.migration_detectors import (
    parse_ddl_statement,
    run_migration_detectors,
)
from pgreviewer.config import settings
from pgreviewer.core.models import (
    ColumnInfo,
    ParsedMigration,
    SchemaInfo,
    Severity,
    TableInfo,
)


def test_alter_column_type_unsafe_flags_critical_on_large_table(monkeypatch):
    monkeypatch.setattr(settings, "TABLE_REWRITE_THRESHOLD", 50_000)

    schema = SchemaInfo(
        tables={
            "users": TableInfo(
                row_estimate=100_000,
                columns=[ColumnInfo(name="status", type="integer")],
            )
        }
    )

    sql = "ALTER TABLE users ALTER COLUMN status TYPE bigint;"
    parsed = ParsedMigration(
        statements=[parse_ddl_statement(sql, 1)],
        source_file="migrations/0001_unsafe_type.sql",
    )

    issues = run_migration_detectors(parsed, schema)
    detector_issues = [i for i in issues if i.detector_name == "alter_column_type"]

    assert len(detector_issues) == 1
    assert detector_issues[0].severity == Severity.CRITICAL
    assert "status" in detector_issues[0].affected_columns


def test_alter_column_type_varchar_broadening_is_safe():
    schema = SchemaInfo(
        tables={
            "products": TableInfo(
                row_estimate=100_000,
                columns=[ColumnInfo(name="name", type="varchar(100)")],
            )
        }
    )

    sql = "ALTER TABLE products ALTER COLUMN name TYPE varchar(200);"
    parsed = ParsedMigration(
        statements=[parse_ddl_statement(sql, 1)],
        source_file="migrations/0002_safe_type.sql",
    )

    issues = run_migration_detectors(parsed, schema)
    detector_issues = [i for i in issues if i.detector_name == "alter_column_type"]

    assert len(detector_issues) == 0


def test_alter_column_type_with_using_is_unsafe():
    schema = SchemaInfo(
        tables={
            "orders": TableInfo(
                row_estimate=60_000,
                columns=[ColumnInfo(name="meta", type="text")],
            )
        }
    )

    sql = "ALTER TABLE orders ALTER COLUMN meta TYPE jsonb USING meta::jsonb;"
    parsed = ParsedMigration(
        statements=[parse_ddl_statement(sql, 1)],
        source_file="migrations/0003_using_clause.sql",
    )

    issues = run_migration_detectors(parsed, schema)
    detector_issues = [i for i in issues if i.detector_name == "alter_column_type"]

    assert len(detector_issues) == 1
    assert detector_issues[0].severity == Severity.CRITICAL


def test_alter_column_type_varchar_to_text_is_unsafe_as_requested():
    schema = SchemaInfo(
        tables={
            "users": TableInfo(
                row_estimate=100_000,
                columns=[ColumnInfo(name="bio", type="varchar(255)")],
            )
        }
    )

    sql = "ALTER TABLE users ALTER COLUMN bio TYPE text;"
    parsed = ParsedMigration(
        statements=[parse_ddl_statement(sql, 1)],
        source_file="migrations/0004_varchar_text.sql",
    )

    issues = run_migration_detectors(parsed, schema)
    detector_issues = [i for i in issues if i.detector_name == "alter_column_type"]

    assert len(detector_issues) == 1
    assert detector_issues[0].severity == Severity.CRITICAL
