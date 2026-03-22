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


def test_add_fk_without_index_flags_warning_no_schema():
    """Without schema data (degraded-static mode), severity is WARNING."""
    schema = SchemaInfo()

    sql = "ALTER TABLE orders ADD COLUMN user_id INTEGER REFERENCES users(id);"
    parsed = ParsedMigration(
        statements=[parse_ddl_statement(sql, 1)],
        source_file="migrations/0001_add_fk.sql",
    )

    issues = run_migration_detectors(parsed, schema)
    detector_issues = [
        i for i in issues if i.detector_name == "add_foreign_key_without_index"
    ]

    assert len(detector_issues) == 1
    assert detector_issues[0].severity == Severity.WARNING
    assert "user_id" in detector_issues[0].affected_columns
    assert "CREATE INDEX CONCURRENTLY" in detector_issues[0].suggested_action


def test_add_fk_without_index_warning_small_table_with_schema():
    """With schema but small table (below threshold), severity stays WARNING."""
    schema = SchemaInfo(tables={"orders": TableInfo(row_estimate=50_000)})

    sql = "ALTER TABLE orders ADD COLUMN user_id INTEGER REFERENCES users(id);"
    parsed = ParsedMigration(
        statements=[parse_ddl_statement(sql, 1)],
        source_file="migrations/0001_add_fk.sql",
    )

    issues = run_migration_detectors(parsed, schema)
    detector_issues = [
        i for i in issues if i.detector_name == "add_foreign_key_without_index"
    ]

    assert len(detector_issues) == 1
    assert detector_issues[0].severity == Severity.WARNING
    assert "user_id" in detector_issues[0].affected_columns
    # Row estimate should appear in description
    assert "50000" in detector_issues[0].description


def test_add_fk_without_index_critical_large_table_with_schema():
    """With schema and large table (above threshold), severity escalates to CRITICAL."""
    schema = SchemaInfo(tables={"orders": TableInfo(row_estimate=500_000)})

    sql = "ALTER TABLE orders ADD COLUMN user_id INTEGER REFERENCES users(id);"
    parsed = ParsedMigration(
        statements=[parse_ddl_statement(sql, 1)],
        source_file="migrations/0001_add_fk.sql",
    )

    issues = run_migration_detectors(parsed, schema)
    detector_issues = [
        i for i in issues if i.detector_name == "add_foreign_key_without_index"
    ]

    assert len(detector_issues) == 1
    assert detector_issues[0].severity == Severity.CRITICAL
    assert "user_id" in detector_issues[0].affected_columns


def test_add_fk_with_index_in_same_migration_is_ignored():
    schema = SchemaInfo()

    statements = [
        parse_ddl_statement(
            "ALTER TABLE orders ADD COLUMN user_id INTEGER REFERENCES users(id);", 1
        ),
        parse_ddl_statement(
            "CREATE INDEX CONCURRENTLY idx_orders_user_id ON orders(user_id);", 2
        ),
    ]
    parsed = ParsedMigration(
        statements=statements,
        source_file="migrations/0002_add_fk_indexed.sql",
    )

    issues = run_migration_detectors(parsed, schema)
    detector_issues = [
        i for i in issues if i.detector_name == "add_foreign_key_without_index"
    ]

    assert len(detector_issues) == 0


def test_add_fk_with_existing_index_in_schema_is_ignored():
    schema = SchemaInfo(
        tables={
            "orders": TableInfo(
                indexes=[IndexInfo(name="idx_old", columns=["user_id"])]
            )
        }
    )

    sql = "ALTER TABLE orders ADD COLUMN user_id INTEGER REFERENCES users(id);"
    parsed = ParsedMigration(
        statements=[parse_ddl_statement(sql, 1)],
        source_file="migrations/0003_add_fk_schema.sql",
    )

    issues = run_migration_detectors(parsed, schema)
    detector_issues = [
        i for i in issues if i.detector_name == "add_foreign_key_without_index"
    ]

    assert len(detector_issues) == 0


def test_add_constraint_fk_without_index_flags_warning_no_schema():
    """ADD CONSTRAINT FK without index → WARNING in degraded-static mode."""
    schema = SchemaInfo()

    # Multiline SQL test
    sql = """
    ALTER TABLE orders
    ADD CONSTRAINT fk_orders_user
    FOREIGN KEY (user_id) REFERENCES users(id);
    """
    parsed = ParsedMigration(
        statements=[parse_ddl_statement(sql, 5)],
        source_file="migrations/0004_add_constraint.sql",
    )

    issues = run_migration_detectors(parsed, schema)
    detector_issues = [
        i for i in issues if i.detector_name == "add_foreign_key_without_index"
    ]

    assert len(detector_issues) == 1
    assert detector_issues[0].severity == Severity.WARNING
    assert "user_id" in detector_issues[0].affected_columns


def test_composite_fk_without_index():
    schema = SchemaInfo()

    sql = (
        "ALTER TABLE order_items ADD CONSTRAINT fk_item "
        "FOREIGN KEY (order_id, sku) REFERENCES products(o, s);"
    )
    parsed = ParsedMigration(
        statements=[parse_ddl_statement(sql, 1)],
        source_file="migrations/0005_composite.sql",
    )

    issues = run_migration_detectors(parsed, schema)
    detector_issues = [
        i for i in issues if i.detector_name == "add_foreign_key_without_index"
    ]

    assert len(detector_issues) == 1
    assert detector_issues[0].affected_columns == ["order_id", "sku"]
    assert "idx_order_items_order_id_sku" in detector_issues[0].suggested_action
