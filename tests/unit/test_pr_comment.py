from datetime import UTC, datetime

from pgreviewer.core.degradation import AnalysisResult
from pgreviewer.core.models import Issue, Severity
from pgreviewer.reporting.pr_comment import _MAX_EXPLAIN_LINES, generate_pr_comment


def _make_issue(
    *,
    severity: Severity,
    detector_name: str,
    context: dict | None = None,
) -> Issue:
    return Issue(
        severity=severity,
        detector_name=detector_name,
        description=f"{detector_name} description",
        affected_table="orders",
        affected_columns=["user_id"],
        suggested_action="CREATE INDEX idx_orders_user_id ON orders(user_id);",
        context=context or {},
    )


def test_generate_pr_comment_no_issues() -> None:
    comment = generate_pr_comment(
        AnalysisResult(), now=datetime(2025, 1, 15, 14, 32, tzinfo=UTC)
    )

    assert comment.startswith("<!-- pgreviewer-report -->")
    assert "## pgreviewer — ✅ No issues found" in comment
    assert "### Summary" in comment
    assert "### Query Performance" in comment
    assert "suppress with `-- pgreviewer:ignore`" in comment
    assert "*Last updated: 2025-01-15 14:32 UTC*" in comment


def test_generate_pr_comment_groups_findings_and_uses_details() -> None:
    result = AnalysisResult(
        issues=[
            _make_issue(severity=Severity.CRITICAL, detector_name="high_cost"),
            _make_issue(
                severity=Severity.WARNING, detector_name="create_index_not_concurrently"
            ),
            _make_issue(
                severity=Severity.WARNING,
                detector_name="cross_cutting_drop_index_query_usage",
            ),
        ]
    )

    comment = generate_pr_comment(result)

    assert "## pgreviewer — 🔴 1 critical, 🟡 2 warnings" in comment
    assert "### Query Performance" in comment
    assert "### Migration Safety" in comment
    assert "### Index Recommendations" in comment
    assert "### Cross-cutting Findings" in comment
    assert "#### `unknown`" in comment
    assert "##### CRITICAL" in comment
    assert comment.count("<details>") == 3
    assert "```sql" in comment


def test_generate_pr_comment_info_only_shows_info_badge() -> None:
    result = AnalysisResult(
        issues=[_make_issue(severity=Severity.INFO, detector_name="high_cost")]
    )

    comment = generate_pr_comment(result)

    assert "## pgreviewer — ℹ️ 1 info" in comment


def test_generate_pr_comment_truncates_long_explain_and_links_full_plan() -> None:
    line_count = 60
    long_plan = "\n".join(f"line {i}" for i in range(1, line_count + 1))
    result = AnalysisResult(
        issues=[
            _make_issue(
                severity=Severity.WARNING,
                detector_name="high_cost",
                context={"explain_plan": long_plan},
            )
        ]
    )

    comment = generate_pr_comment(result)

    hidden_lines = line_count - _MAX_EXPLAIN_LINES
    assert f"... (truncated {hidden_lines} lines)" in comment
    assert "[show full plan](#full-plan-1)" in comment
    assert "<summary>Full EXPLAIN plan</summary>" in comment


def test_generate_pr_comment_renders_workload_stats_context() -> None:
    result = AnalysisResult(
        issues=[
            _make_issue(
                severity=Severity.WARNING,
                detector_name="high_cost",
                context={
                    "workload_stats": {
                        "calls_per_day": 4832,
                        "avg_time_ms": 312,
                        "total_time_min_per_day": 25.1,
                    }
                },
            )
        ]
    )

    comment = generate_pr_comment(result)

    assert "⚡ **Production workload match:**" in comment
    assert "Calls: 4,832/day | Avg time: 312ms | Total time: 25.1 min/day" in comment
