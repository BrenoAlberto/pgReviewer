from pgreviewer.analysis.migration_detectors import (
    parse_ddl_statement,
    run_migration_detectors,
)
from pgreviewer.config import settings
from pgreviewer.core.models import (
    IndexInfo,
    ParsedMigration,
    SchemaInfo,
    Severity,
    TableInfo,
)


def test_large_table_ddl_warning_on_huge_table(monkeypatch):
    monkeypatch.setattr(settings, "LARGE_TABLE_DDL_THRESHOLD", 10_000_000)

    schema = SchemaInfo(
        tables={
            "analytics_events": TableInfo(row_estimate=15_000_000),
            "small_table": TableInfo(row_estimate=100_000),
        }
    )

    # 1. ALTER TABLE on large table
    sql1 = "ALTER TABLE analytics_events ADD COLUMN context JSONB;"
    parsed1 = ParsedMigration(
        statements=[parse_ddl_statement(sql1, 1)],
        source_file="migrations/0001_large.sql",
    )
    issues1 = run_migration_detectors(parsed1, schema)
    large_issues1 = [i for i in issues1 if i.detector_name == "large_table_ddl"]
    assert len(large_issues1) == 1
    assert large_issues1[0].severity == Severity.WARNING
    assert "15,000,000" in large_issues1[0].description

    # 2. CREATE INDEX on large table
    sql2 = "CREATE INDEX idx_events_ts ON analytics_events(created_at);"
    parsed2 = ParsedMigration(
        statements=[parse_ddl_statement(sql2, 1)],
        source_file="migrations/0001_large.sql",
    )
    issues2 = run_migration_detectors(parsed2, schema)
    large_issues2 = [i for i in issues2 if i.detector_name == "large_table_ddl"]
    assert len(large_issues2) == 1

    # 3. DROP INDEX on large table (needs schema lookup)
    schema_with_idx = SchemaInfo(
        tables={
            "analytics_events": TableInfo(
                row_estimate=15_000_000,
                indexes=[IndexInfo(name="idx_old", columns=["old_col"])],
            )
        }
    )
    sql3 = "DROP INDEX idx_old;"
    parsed3 = ParsedMigration(
        statements=[parse_ddl_statement(sql3, 1)],
        source_file="migrations/0001_large.sql",
    )
    issues3 = run_migration_detectors(parsed3, schema_with_idx)
    large_issues3 = [i for i in issues3 if i.detector_name == "large_table_ddl"]
    assert len(large_issues3) == 1
    assert large_issues3[0].affected_table == "analytics_events"


def test_large_table_ddl_no_issue_on_moderate_table(monkeypatch):
    monkeypatch.setattr(settings, "LARGE_TABLE_DDL_THRESHOLD", 10_000_000)

    schema = SchemaInfo(tables={"orders": TableInfo(row_estimate=500_000)})

    sql = "ALTER TABLE orders ADD COLUMN notes TEXT;"
    parsed = ParsedMigration(
        statements=[parse_ddl_statement(sql, 1)],
        source_file="migrations/0002_moderate.sql",
    )
    issues = run_migration_detectors(parsed, schema)
    large_issues = [i for i in issues if i.detector_name == "large_table_ddl"]
    assert len(large_issues) == 0
