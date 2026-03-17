from __future__ import annotations

from unittest.mock import patch

from pgreviewer.core.degradation import AnalysisResult
from pgreviewer.core.models import IndexRecommendation, Issue, Severity
from pgreviewer.reporting.cli_report import generate_cli_report
from pgreviewer.reporting.pr_comment import generate_pr_comment
from pgreviewer.reporting.sections import SectionType, build_report_sections


def _issue(severity: Severity, detector: str, context: dict | None = None) -> Issue:
    return Issue(
        severity=severity,
        detector_name=detector,
        description=f"{detector} issue",
        affected_table="orders",
        affected_columns=["user_id"],
        suggested_action="CREATE INDEX idx_orders_user_id ON orders(user_id);",
        context=context or {},
    )


def test_build_report_sections_groups_query_findings_by_file_then_severity() -> None:
    result = AnalysisResult(
        issues=[
            _issue(Severity.WARNING, "high_cost", {"query_file": "b.py"}),
            _issue(Severity.CRITICAL, "high_cost", {"query_file": "a.py"}),
            _issue(Severity.WARNING, "create_index_not_concurrently"),
            _issue(Severity.INFO, "cross_cutting_drop_index_query_usage"),
        ],
        queries_analyzed=4,
    )

    sections = build_report_sections(result)
    assert [section.section_type for section in sections] == [
        SectionType.SUMMARY,
        SectionType.QUERY_PERFORMANCE,
        SectionType.MIGRATION_SAFETY,
        SectionType.INDEX_RECOMMENDATIONS,
        SectionType.CROSS_CUTTING,
    ]

    summary = sections[0]
    assert any(
        f.title == "Queries analyzed" and f.detail == "4"
        for f in summary.findings
    )

    query = sections[1]
    assert [finding.source_file for finding in query.findings] == ["a.py", "b.py"]
    assert [finding.severity for finding in query.findings] == [
        Severity.CRITICAL,
        Severity.WARNING,
    ]


def test_build_report_sections_deduplicates_and_ranks_recommendations() -> None:
    base = IndexRecommendation(
        table="orders",
        columns=["user_id"],
        create_statement="CREATE INDEX idx_orders_user_id ON orders(user_id);",
        improvement_pct=0.2,
    )
    better_duplicate = IndexRecommendation(
        table="orders",
        columns=["user_id"],
        create_statement="CREATE INDEX idx_orders_user_id2 ON orders(user_id);",
        improvement_pct=0.5,
    )
    other = IndexRecommendation(
        table="users",
        columns=["email"],
        create_statement="CREATE INDEX idx_users_email ON users(email);",
        improvement_pct=0.3,
    )
    result = AnalysisResult(recommendations=[base, better_duplicate, other])

    index_section = build_report_sections(result)[3]
    assert index_section.section_type == SectionType.INDEX_RECOMMENDATIONS
    assert len(index_section.findings) == 2
    assert index_section.findings[0].recommendation.improvement_pct == 0.5
    assert index_section.findings[1].recommendation.improvement_pct == 0.3


def test_renderers_use_shared_sections_builder() -> None:
    result = AnalysisResult()

    with patch("pgreviewer.reporting.pr_comment.build_report_sections") as pr_builder:
        pr_builder.return_value = build_report_sections(result)
        generate_pr_comment(result)
        pr_builder.assert_called_once_with(result)

    with patch("pgreviewer.reporting.cli_report.build_report_sections") as cli_builder:
        cli_builder.return_value = build_report_sections(result)
        generate_cli_report(result)
        cli_builder.assert_called_once_with(result)
