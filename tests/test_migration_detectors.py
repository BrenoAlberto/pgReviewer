from pgreviewer.analysis.issue_detectors import DetectorRegistry
from pgreviewer.analysis.migration_detectors import (
    parse_ddl_statement,
    run_migration_detectors,
)
from pgreviewer.core.models import ParsedMigration, SchemaInfo


def test_detector_registry_discovers_migration_detectors():
    detector_names = {
        detector.name for detector in DetectorRegistry().migration_detectors()
    }
    assert "destructive_ddl" in detector_names


def test_run_migration_detectors_flags_drop_column():
    parsed = ParsedMigration(
        statements=[parse_ddl_statement("ALTER TABLE users DROP COLUMN email;", 12)],
        source_file="migrations/0002_drop_email.sql",
    )

    issues = run_migration_detectors(parsed, SchemaInfo())

    assert len(issues) == 1
    assert issues[0].detector_name == "destructive_ddl"
    assert issues[0].affected_table == "users"
