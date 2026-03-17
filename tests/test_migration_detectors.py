from pgreviewer.analysis.issue_detectors import DetectorRegistry
from pgreviewer.analysis.migration_detectors import (
    parse_ddl_statement,
    run_migration_detectors,
)
from pgreviewer.config import settings
from pgreviewer.core.models import ExtractedQuery, ParsedMigration, SchemaInfo, Severity


def test_detector_registry_discovers_migration_detectors():
    detector_names = {
        detector.name for detector in DetectorRegistry().migration_detectors()
    }
    assert "destructive_ddl" in detector_names
    assert "add_column_with_default" in detector_names
    assert "drop_column_still_referenced" in detector_names


def test_run_migration_detectors_does_not_flag_unreferenced_drop_column():
    parsed = ParsedMigration(
        statements=[parse_ddl_statement("ALTER TABLE users DROP COLUMN email;", 12)],
        source_file="migrations/0002_drop_email.sql",
    )

    issues = run_migration_detectors(parsed, SchemaInfo())

    assert issues == []


def test_run_migration_detectors_flags_referenced_drop_column():
    parsed = ParsedMigration(
        statements=[
            parse_ddl_statement("ALTER TABLE orders DROP COLUMN legacy_id;", 4)
        ],
        source_file="migrations/0007_drop_legacy_id.sql",
        extracted_queries=[
            ExtractedQuery(
                sql="ALTER TABLE orders DROP COLUMN legacy_id;",
                source_file="migrations/0007_drop_legacy_id.sql",
                line_number=4,
                extraction_method="migration_sql",
                confidence=1.0,
            ),
            ExtractedQuery(
                sql="SELECT legacy_id FROM orders WHERE legacy_id IS NOT NULL;",
                source_file="app/orders_repo.py",
                line_number=18,
                extraction_method="ast",
                confidence=0.95,
            ),
        ],
    )

    issues = run_migration_detectors(parsed, SchemaInfo())
    detector_issues = [
        i for i in issues if i.detector_name == "drop_column_still_referenced"
    ]

    assert len(detector_issues) == 1
    assert detector_issues[0].severity == Severity.CRITICAL
    assert detector_issues[0].affected_table == "orders"
    assert detector_issues[0].affected_columns == ["legacy_id"]
    assert "app/orders_repo.py:18" in detector_issues[0].description


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
