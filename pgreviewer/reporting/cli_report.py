from __future__ import annotations

from typing import TYPE_CHECKING

from pgreviewer.reporting.sections import SectionType, build_report_sections
from pgreviewer.reporting.workload import format_workload_stats

if TYPE_CHECKING:
    from pgreviewer.core.degradation import AnalysisResult


def generate_cli_report(result: AnalysisResult) -> str:
    lines: list[str] = []
    sections = build_report_sections(result)

    for section in sections:
        lines.append(f"{section.title}")
        if not section.findings:
            lines.append("  - No findings.")
            lines.append("")
            continue

        if section.section_type == SectionType.SUMMARY:
            for finding in section.findings:
                lines.append(f"  - {finding.title}: {finding.detail}")
        elif section.section_type == SectionType.QUERY_PERFORMANCE:
            current_file: str | None = None
            current_severity: str | None = None
            for finding in section.findings:
                file_name = finding.source_file or "unknown"
                if file_name != current_file:
                    current_file = file_name
                    current_severity = None
                    lines.append(f"  {file_name}")
                sev = finding.severity.value if finding.severity is not None else "INFO"
                if sev != current_severity:
                    current_severity = sev
                    lines.append(f"    {sev}")
                issue = finding.issue
                if issue is None:
                    continue
                lines.append(
                    f"      - {issue.detector_name}: {issue.description} | "
                    f"action: {issue.suggested_action}"
                )
                workload_detail = format_workload_stats(issue.context or {})
                if workload_detail is not None:
                    lines.append("        ⚡ Production workload match:")
                    lines.append(f"           {workload_detail}")
        elif section.section_type == SectionType.INDEX_RECOMMENDATIONS:
            lines.append("  Recommended Indexes")
            for finding in section.findings:
                recommendation = finding.recommendation
                if recommendation is None:
                    continue
                lines.append(
                    f"    - Suggested index: {finding.title} ({finding.detail}, "
                    f"validated={'yes' if recommendation.validated else 'no'})"
                )
        else:
            for finding in section.findings:
                issue = finding.issue
                if issue is None:
                    continue
                lines.append(
                    f"  - {issue.severity.value} {issue.detector_name}: "
                    f"{issue.description} | action: {issue.suggested_action}"
                )

        lines.append("")

    return "\n".join(lines).rstrip()
