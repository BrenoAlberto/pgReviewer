from pgreviewer.analysis.cross_correlator import correlate_findings
from pgreviewer.core.models import ExtractedQuery, Issue, Severity


def test_correlate_add_column_and_filter_query_without_index():
    migration_query = ExtractedQuery(
        sql="ALTER TABLE orders ADD COLUMN status VARCHAR;",
        source_file="migrations/001_add_status.sql",
        line_number=3,
        extraction_method="migration_sql",
        confidence=1.0,
    )
    app_query = ExtractedQuery(
        sql="SELECT * FROM orders WHERE status = 'active';",
        source_file="app/orders_repo.py",
        line_number=12,
        extraction_method="ast",
        confidence=0.9,
    )
    missing_index_issue = Issue(
        severity=Severity.WARNING,
        detector_name="missing_index_on_filter",
        description=(
            "Seq Scan on 'orders' filters on ['status'] "
            "but no covering index exists"
        ),
        affected_table="orders",
        affected_columns=["status"],
        suggested_action="Consider adding an index on orders(status)",
    )
    results = [
        {"query_obj": migration_query, "issues": [], "recs": []},
        {"query_obj": app_query, "issues": [missing_index_issue], "recs": []},
    ]

    findings = correlate_findings(results)

    assert len(findings) == 1
    assert findings[0].issue.severity == Severity.CRITICAL
    assert (
        findings[0].issue.detector_name
        == "cross_cutting_add_column_query_without_index"
    )
    assert findings[0].migration_file == "migrations/001_add_status.sql"
    assert findings[0].query_file == "app/orders_repo.py"
    assert results[1]["issues"] == []


def test_correlate_drop_index_with_query_usage():
    migration_query = ExtractedQuery(
        sql="DROP INDEX idx_orders_status;",
        source_file="migrations/002_drop_status_idx.sql",
        line_number=7,
        extraction_method="migration_sql",
        confidence=1.0,
    )
    app_query = ExtractedQuery(
        sql="SELECT * FROM orders WHERE status = 'active';",
        source_file="app/orders_repo.py",
        line_number=22,
        extraction_method="ast",
        confidence=0.9,
    )
    missing_index_issue = Issue(
        severity=Severity.WARNING,
        detector_name="missing_index_on_filter",
        description="No index",
        affected_table="orders",
        affected_columns=["status"],
        suggested_action="Create index",
    )
    results = [
        {"query_obj": migration_query, "issues": [], "recs": []},
        {"query_obj": app_query, "issues": [missing_index_issue], "recs": []},
    ]

    findings = correlate_findings(results)

    assert len(findings) == 1
    assert findings[0].issue.detector_name == "cross_cutting_drop_index_query_usage"
    assert results[1]["issues"] == []


def test_fk_without_index_join_is_deduplicated():
    migration_query = ExtractedQuery(
        sql="ALTER TABLE orders ADD COLUMN user_id INTEGER REFERENCES users(id);",
        source_file="migrations/003_add_fk.sql",
        line_number=4,
        extraction_method="migration_sql",
        confidence=1.0,
    )
    app_query = ExtractedQuery(
        sql="SELECT o.id FROM orders o JOIN users u ON o.user_id = u.id;",
        source_file="app/orders_repo.py",
        line_number=33,
        extraction_method="ast",
        confidence=0.9,
    )
    fk_issue = Issue(
        severity=Severity.CRITICAL,
        detector_name="add_foreign_key_without_index",
        description="Foreign key columns ['user_id'] on table 'orders' are not indexed",
        affected_table="orders",
        affected_columns=["user_id"],
        suggested_action=(
            "CREATE INDEX CONCURRENTLY idx_orders_user_id ON orders (user_id);"
        ),
    )
    results = [
        {"query_obj": migration_query, "issues": [fk_issue], "recs": []},
        {"query_obj": app_query, "issues": [], "recs": []},
    ]

    findings = correlate_findings(results)

    assert len(findings) == 1
    assert findings[0].issue.detector_name == "cross_cutting_fk_without_index_join"
    assert results[0]["issues"] == []
