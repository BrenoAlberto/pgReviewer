from pgreviewer.analysis.migration_detectors import parse_ddl_statement
from pgreviewer.analysis.migration_detectors.drop_index_workload import (
    detect_drop_index_workload_issues,
)
from pgreviewer.core.models import ParsedMigration, Severity, SlowQuery


def test_drop_index_workload_emits_critical_issue_when_slow_query_matches_column():
    migration = ParsedMigration(
        statements=[parse_ddl_statement("DROP INDEX idx_orders_created_at;", 4)],
        source_file="migrations/010_drop_idx.sql",
    )
    slow_queries = [
        SlowQuery(
            query_text="SELECT * FROM orders WHERE created_at > $1;",
            calls=300,
            mean_exec_time_ms=18.0,
            total_exec_time_ms=5_400.0,
            rows=5_000,
        )
    ]

    issues = detect_drop_index_workload_issues(migration, slow_queries)

    assert len(issues) == 1
    assert issues[0].severity == Severity.CRITICAL
    assert issues[0].detector_name == "drop_index_workload"


def test_drop_index_workload_no_matching_slow_queries_produces_no_issue():
    migration = ParsedMigration(
        statements=[parse_ddl_statement("DROP INDEX idx_orders_created_at;", 4)],
        source_file="migrations/010_drop_idx.sql",
    )
    slow_queries = [
        SlowQuery(
            query_text="SELECT * FROM orders WHERE status = $1;",
            calls=300,
            mean_exec_time_ms=18.0,
            total_exec_time_ms=5_400.0,
            rows=5_000,
        )
    ]

    issues = detect_drop_index_workload_issues(migration, slow_queries)

    assert issues == []
