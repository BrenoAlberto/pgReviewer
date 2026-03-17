from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pgreviewer.core.models import IndexRecommendation, Issue, Severity
from pgreviewer.exceptions import LLMUnavailableError, StructuredOutputError
from pgreviewer.llm.prompts.report_summarizer import summarize_report

if TYPE_CHECKING:
    from pgreviewer.core.degradation import AnalysisResult

_MIGRATION_DETECTORS = {
    "add_foreign_key_without_index",
    "add_column_with_default",
    "destructive_ddl",
    "alter_column_type",
    "large_table_ddl",
    "create_index_not_concurrently",
    "add_not_null_without_default",
    "drop_column_still_referenced",
}

_SEVERITY_ORDER = {
    Severity.CRITICAL: 0,
    Severity.WARNING: 1,
    Severity.INFO: 2,
}


class SectionType(StrEnum):
    QUERY_PERFORMANCE = "QUERY_PERFORMANCE"
    MIGRATION_SAFETY = "MIGRATION_SAFETY"
    INDEX_RECOMMENDATIONS = "INDEX_RECOMMENDATIONS"
    CROSS_CUTTING = "CROSS_CUTTING"
    SUMMARY = "SUMMARY"


@dataclass
class Finding:
    title: str
    detail: str = ""
    severity: Severity | None = None
    issue: Issue | None = None
    recommendation: IndexRecommendation | None = None
    source_file: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ReportSection:
    title: str
    severity: Severity | None
    findings: list[Finding]
    section_type: SectionType


def _source_file_for(issue: Issue) -> str:
    context = issue.context or {}
    for key in ("query_file", "file", "source_file", "migration_file"):
        value = context.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return "unknown"


def _category_for(issue: Issue) -> SectionType:
    if issue.detector_name.startswith("cross_cutting_"):
        return SectionType.CROSS_CUTTING
    if issue.detector_name in _MIGRATION_DETECTORS:
        return SectionType.MIGRATION_SAFETY
    return SectionType.QUERY_PERFORMANCE


def _section_severity(findings: list[Finding]) -> Severity | None:
    severities = [f.severity for f in findings if f.severity is not None]
    if not severities:
        return None
    return min(severities, key=lambda severity: _SEVERITY_ORDER.get(severity, 99))


def _deduplicate_recommendations(
    recommendations: list[IndexRecommendation],
) -> list[IndexRecommendation]:
    deduped: dict[
        tuple[str, tuple[str, ...], str, bool, str | None], IndexRecommendation
    ] = {}
    for rec in recommendations:
        key = (
            rec.table,
            tuple(rec.columns),
            rec.index_type,
            rec.is_unique,
            rec.partial_predicate,
        )
        current = deduped.get(key)
        if current is None or rec.improvement_pct > current.improvement_pct:
            deduped[key] = rec
    return sorted(deduped.values(), key=lambda r: r.improvement_pct, reverse=True)


def build_report_sections(result: AnalysisResult) -> list[ReportSection]:
    critical = sum(1 for issue in result.issues if issue.severity == Severity.CRITICAL)
    warning = sum(1 for issue in result.issues if issue.severity == Severity.WARNING)
    info = sum(1 for issue in result.issues if issue.severity == Severity.INFO)
    status = "PASS" if not result.issues else "FAIL"
    llm_note = "LLM used" if result.llm_used else "LLM not used"
    if result.llm_degraded:
        llm_reason = result.degradation_reason or "algorithmic-only analysis"
        llm_note = f"LLM degraded: {llm_reason}"

    summary_findings = [
        Finding(title="Status", detail=status),
        Finding(
            title="Issue counts",
            detail=f"critical={critical}, warning={warning}, info={info}",
        ),
        Finding(title="Queries analyzed", detail=str(result.queries_analyzed)),
        Finding(title="LLM", detail=llm_note),
    ]
    should_use_llm_summary = (
        len(result.issues) >= 3 or result.llm_interpretation is not None
    )
    if should_use_llm_summary:
        try:
            report_summary = summarize_report(result.issues, result.llm_interpretation)
        except (LLMUnavailableError, StructuredOutputError):
            report_summary = None
        if report_summary is not None:
            summary_findings.append(
                Finding(title="Business impact", detail=report_summary.summary)
            )

    section_findings: dict[SectionType, list[Finding]] = {
        SectionType.QUERY_PERFORMANCE: [],
        SectionType.MIGRATION_SAFETY: [],
        SectionType.CROSS_CUTTING: [],
    }
    for issue in result.issues:
        section_findings[_category_for(issue)].append(
            Finding(
                title=issue.description,
                detail=issue.suggested_action,
                severity=issue.severity,
                issue=issue,
                source_file=_source_file_for(issue),
            )
        )

    query_findings = sorted(
        section_findings[SectionType.QUERY_PERFORMANCE],
        key=lambda finding: (
            finding.source_file or "",
            _SEVERITY_ORDER.get(finding.severity or Severity.INFO, 99),
            finding.issue.detector_name if finding.issue else "",
        ),
    )

    deduped_recs = _deduplicate_recommendations(result.recommendations)
    recommendation_findings = [
        Finding(
            title=rec.create_statement
            or f"CREATE INDEX ON {rec.table}({', '.join(rec.columns)})",
            detail=f"improvement={(rec.improvement_pct * 100):.1f}%",
            recommendation=rec,
        )
        for rec in deduped_recs
    ]

    migration_findings = section_findings[SectionType.MIGRATION_SAFETY]
    cross_cutting_findings = section_findings[SectionType.CROSS_CUTTING]

    return [
        ReportSection(
            title="Summary",
            severity=None
            if status == "PASS"
            else _section_severity(
                query_findings + migration_findings + cross_cutting_findings
            ),
            findings=summary_findings,
            section_type=SectionType.SUMMARY,
        ),
        ReportSection(
            title="Query Performance",
            severity=_section_severity(query_findings),
            findings=query_findings,
            section_type=SectionType.QUERY_PERFORMANCE,
        ),
        ReportSection(
            title="Migration Safety",
            severity=_section_severity(migration_findings),
            findings=migration_findings,
            section_type=SectionType.MIGRATION_SAFETY,
        ),
        ReportSection(
            title="Index Recommendations",
            severity=None,
            findings=recommendation_findings,
            section_type=SectionType.INDEX_RECOMMENDATIONS,
        ),
        ReportSection(
            title="Cross-cutting Findings",
            severity=_section_severity(cross_cutting_findings),
            findings=cross_cutting_findings,
            section_type=SectionType.CROSS_CUTTING,
        ),
    ]
