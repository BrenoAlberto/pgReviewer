from __future__ import annotations

from pgreviewer.analysis.cross_correlator import correlate_findings
from pgreviewer.core.degradation import AnalysisResult
from pgreviewer.core.models import ExtractedQuery, Issue, Severity


def _make_result(
    sql: str,
    source_file: str,
    line_number: int,
    extraction_method: str = "migration_sql",
    issues: list[Issue] | None = None,
) -> dict:
    q = ExtractedQuery(
        sql=sql,
        source_file=source_file,
        line_number=line_number,
        extraction_method=extraction_method,
        confidence=1.0,
    )
    return {
        "query_obj": q,
        "analysis_result": AnalysisResult(issues=issues or []),
        "issues": issues or [],
        "recs": [],
    }


def _seq_scan_issue(table: str, column: str) -> Issue:
    return Issue(
        severity=Severity.WARNING,
        detector_name="missing_index_on_filter",
        description=f"seq scan on {table}.{column}",
        affected_table=table,
        affected_columns=[column],
        suggested_action="add index",
    )


# ── drop_index cross-cutting ──────────────────────────────────────────────────


def test_dropped_index_cause_fields():
    # Index name "idx_orders_status": _infer_index_columns strips "idx", then "orders"
    # leaving ["status"] — which matches the query issue's affected_columns.
    migration = _make_result(
        sql="DROP INDEX idx_orders_status",
        source_file="migrations/0002_drop_idx.py",
        line_number=7,
    )
    query_issue = _seq_scan_issue("orders", "status")
    query = _make_result(
        sql="SELECT * FROM orders WHERE status = $1",
        source_file="app/queries.py",
        line_number=42,
        extraction_method="ast",
        issues=[query_issue],
    )

    findings = correlate_findings([migration, query])

    drop_findings = [
        f
        for f in findings
        if f.issue.detector_name == "cross_cutting_drop_index_query_usage"
    ]
    assert drop_findings, "expected a cross-cutting drop_index finding"
    issue = drop_findings[0].issue
    assert issue.cause_file == "migrations/0002_drop_idx.py"
    assert issue.cause_line == 7
    assert issue.cause_context is not None
    assert "idx_orders_status" in issue.cause_context
    assert "dropped here" in issue.cause_context


# ── add_column cross-cutting ──────────────────────────────────────────────────


def test_added_column_cause_fields():
    migration = _make_result(
        sql="ALTER TABLE orders ADD COLUMN referral_code VARCHAR(50)",
        source_file="migrations/0003_add_col.py",
        line_number=5,
    )
    query_issue = _seq_scan_issue("orders", "referral_code")
    query = _make_result(
        sql="SELECT * FROM orders WHERE referral_code = $1",
        source_file="app/queries.py",
        line_number=20,
        extraction_method="ast",
        issues=[query_issue],
    )

    findings = correlate_findings([migration, query])

    add_findings = [
        f
        for f in findings
        if f.issue.detector_name == "cross_cutting_add_column_query_without_index"
    ]
    assert add_findings, "expected a cross-cutting add_column finding"
    issue = add_findings[0].issue
    assert issue.cause_file == "migrations/0003_add_col.py"
    assert issue.cause_line == 5
    assert issue.cause_context is not None
    assert "referral_code" in issue.cause_context
    assert "added here" in issue.cause_context
