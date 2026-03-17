from pgreviewer.analysis.issue_detectors import DetectorRegistry
from pgreviewer.analysis.migration_detectors import (
    parse_ddl_statement,
    run_migration_detectors,
)
from pgreviewer.config import settings
from pgreviewer.core.models import ParsedMigration, SchemaInfo, Severity


def test_detector_registry_discovers_migration_detectors():
    detector_names = {
        detector.name for detector in DetectorRegistry().migration_detectors()
    }
    assert "destructive_ddl" in detector_names
    assert "add_column_with_default" in detector_names


def test_run_migration_detectors_flags_drop_column():
    parsed = ParsedMigration(
        statements=[parse_ddl_statement("ALTER TABLE users DROP COLUMN email;", 12)],
        source_file="migrations/0002_drop_email.sql",
    )

    issues = run_migration_detectors(parsed, SchemaInfo())

    assert len(issues) == 1
    assert issues[0].detector_name == "destructive_ddl"
    assert issues[0].affected_table == "users"


def test_add_column_default_is_critical_on_postgres_10(monkeypatch):
    monkeypatch.setattr(settings, "POSTGRES_VERSION", 10)
    parsed = ParsedMigration(
        statements=[
            parse_ddl_statement(
                "ALTER TABLE users ADD COLUMN created_at TIMESTAMPTZ DEFAULT NOW();", 7
            )
        ],
        source_file="migrations/0003_add_created_at.sql",
    )

    issues = run_migration_detectors(parsed, SchemaInfo())
    detector_issues = [
        i for i in issues if i.detector_name == "add_column_with_default"
    ]

    assert len(detector_issues) == 1
    assert detector_issues[0].severity == Severity.CRITICAL


def test_add_column_default_is_warning_on_postgres_11_with_volatile_default(
    monkeypatch,
):
    monkeypatch.setattr(settings, "POSTGRES_VERSION", 11)
    parsed = ParsedMigration(
        statements=[
            parse_ddl_statement(
                "ALTER TABLE users ADD COLUMN token UUID DEFAULT uuid_generate_v4();", 9
            )
        ],
        source_file="migrations/0004_add_token.sql",
    )

    issues = run_migration_detectors(parsed, SchemaInfo())
    detector_issues = [
        i for i in issues if i.detector_name == "add_column_with_default"
    ]

    assert len(detector_issues) == 1
    assert detector_issues[0].severity == Severity.WARNING


def test_add_column_default_is_info_on_postgres_11_with_literal_default(monkeypatch):
    monkeypatch.setattr(settings, "POSTGRES_VERSION", 11)
    parsed = ParsedMigration(
        statements=[
            parse_ddl_statement(
                "ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'pending';", 11
            )
        ],
        source_file="migrations/0005_add_status.sql",
    )

    issues = run_migration_detectors(parsed, SchemaInfo())
    detector_issues = [
        i for i in issues if i.detector_name == "add_column_with_default"
    ]

    assert len(detector_issues) == 1
    assert detector_issues[0].severity == Severity.INFO
