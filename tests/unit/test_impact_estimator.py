from pgreviewer.analysis.impact_estimator import estimate_loop_impact
from pgreviewer.core.models import Issue, SchemaInfo, Severity, TableInfo


def test_estimate_loop_impact_uses_schema_row_estimate_for_query_iterable() -> None:
    issue = Issue(
        severity=Severity.CRITICAL,
        detector_name="query_in_loop",
        description="Query in loop",
        affected_table=None,
        affected_columns=[],
        suggested_action="Batch query",
        context={"iterable_source_table": "orders"},
    )
    schema = SchemaInfo(tables={"orders": TableInfo(row_estimate=250_000)})

    estimate = estimate_loop_impact(issue, schema)

    assert estimate.max_iterations == 250_000
    assert estimate.source_table == "orders"
    assert estimate.source_row_estimate == 250_000
    assert estimate.estimated_extra_queries == 250_000
    assert estimate.estimated_time_overhead_ms == 250_000
    assert "orders table has 250,000 rows" in estimate.summary
    assert "~250 seconds of DB time" in estimate.summary


def test_estimate_loop_impact_reports_unknown_when_source_is_not_identified() -> None:
    issue = Issue(
        severity=Severity.CRITICAL,
        detector_name="query_in_loop",
        description="Query in loop",
        affected_table=None,
        affected_columns=[],
        suggested_action="Batch query",
        context={"iterable": "items"},
    )

    estimate = estimate_loop_impact(issue, SchemaInfo())

    assert estimate.max_iterations is None
    assert estimate.source_table is None
    assert estimate.estimated_extra_queries is None
    assert estimate.estimated_time_overhead_ms is None
    assert estimate.requires_manual_review is True
    assert estimate.summary == "unknown iteration count — review manually"
