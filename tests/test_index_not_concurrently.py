from pgreviewer.analysis.migration_detectors import (
    parse_ddl_statement,
    run_migration_detectors,
)
from pgreviewer.config import settings
from pgreviewer.core.models import ParsedMigration, SchemaInfo, Severity, TableInfo


def test_create_index_not_concurrently_flags_critical_on_large_table(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(settings, "CONCURRENT_INDEX_THRESHOLD", 100_000)

    schema = SchemaInfo(tables={"orders": TableInfo(row_estimate=500_000)})

    # Create a dummy file to avoid _check_if_transactional reading a non-existent file
    sql_file = tmp_path / "0001_add_idx.sql"
    sql_file.write_text("CREATE INDEX idx_orders_user_id ON orders(user_id);")

    parsed = ParsedMigration(
        statements=[
            parse_ddl_statement(
                "CREATE INDEX idx_orders_user_id ON orders(user_id);", 1
            )
        ],
        source_file=str(sql_file),
    )

    issues = run_migration_detectors(parsed, schema)
    detector_issues = [
        i for i in issues if i.detector_name == "create_index_not_concurrently"
    ]

    assert len(detector_issues) == 1
    assert detector_issues[0].severity == Severity.CRITICAL
    assert "CONCURRENTLY" in detector_issues[0].suggested_action
    assert "orders" in detector_issues[0].affected_table


def test_create_index_concurrently_is_ignored(tmp_path):
    schema = SchemaInfo(tables={"orders": TableInfo(row_estimate=500_000)})

    sql_file = tmp_path / "0002_add_idx_concurrently.sql"
    sql_file.write_text(
        "CREATE INDEX CONCURRENTLY idx_orders_user_id ON orders(user_id);"
    )

    parsed = ParsedMigration(
        statements=[
            parse_ddl_statement(
                "CREATE INDEX CONCURRENTLY idx_orders_user_id ON orders(user_id);", 1
            )
        ],
        source_file=str(sql_file),
    )

    issues = run_migration_detectors(parsed, schema)
    detector_issues = [
        i for i in issues if i.detector_name == "create_index_not_concurrently"
    ]

    assert len(detector_issues) == 0


def test_create_index_concurrently_inside_transaction_flags_warning(tmp_path):
    schema = SchemaInfo(tables={"orders": TableInfo(row_estimate=500_000)})

    sql_file = tmp_path / "0003_transactional.sql"
    sql_file.write_text(
        "BEGIN;\n"
        "CREATE INDEX CONCURRENTLY idx_orders_user_id ON orders(user_id);\n"
        "COMMIT;"
    )

    parsed = ParsedMigration(
        statements=[
            parse_ddl_statement(
                "CREATE INDEX CONCURRENTLY idx_orders_user_id ON orders(user_id);", 2
            )
        ],
        source_file=str(sql_file),
    )

    issues = run_migration_detectors(parsed, schema)
    detector_issues = [
        i for i in issues if i.detector_name == "create_index_not_concurrently"
    ]

    assert len(detector_issues) == 1
    assert detector_issues[0].severity == Severity.WARNING
    assert (
        "cannot run inside a transaction block" in detector_issues[0].suggested_action
    )
