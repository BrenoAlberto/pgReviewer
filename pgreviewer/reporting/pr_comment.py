from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pgreviewer.reporting.sections import SectionType, build_report_sections

if TYPE_CHECKING:
    from pgreviewer.core.degradation import AnalysisResult
    from pgreviewer.core.models import Issue

_SQL_PREFIX_RE = re.compile(
    r"^\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|WITH)\b", re.IGNORECASE
)

_DOCS_URL = "https://github.com/BrenoAlberto/pgReviewer#readme"
_MAX_EXPLAIN_LINES = 50
REPORT_SIGNATURE = "<!-- pgreviewer-report -->"


def _count_label(count: int, label: str) -> str:
    suffix = "s" if count != 1 else ""
    return f"{count} {label}{suffix}"


def _risk_badge(issues: list[Issue]) -> str:
    critical = sum(1 for i in issues if i.severity.value == "CRITICAL")
    warning = sum(1 for i in issues if i.severity.value == "WARNING")
    info = sum(1 for i in issues if i.severity.value == "INFO")

    parts: list[str] = []
    if critical:
        parts.append(f"🔴 {_count_label(critical, 'critical')}")
    if warning:
        parts.append(f"🟡 {_count_label(warning, 'warning')}")
    if info:
        parts.append(f"ℹ️ {_count_label(info, 'info')}")

    return ", ".join(parts) if parts else "✅ No issues found"


def _extract_sql_blocks(issue: Issue) -> list[str]:
    sql_blocks: list[str] = []
    context = issue.context or {}

    for key in ("sql", "query", "raw_sql", "statement", "suggested_sql"):
        value = context.get(key)
        if isinstance(value, str) and _SQL_PREFIX_RE.search(value):
            sql_blocks.append(value.strip())

    action = issue.suggested_action.strip()
    if _SQL_PREFIX_RE.search(action):
        sql_blocks.append(action)

    deduped: list[str] = []
    seen: set[str] = set()
    for block in sql_blocks:
        if block in seen:
            continue
        seen.add(block)
        deduped.append(block)
    return deduped


def _render_explain_plan(plan: object, finding_idx: int) -> str:
    if isinstance(plan, str):
        plan_text = plan
    elif isinstance(plan, list) and all(isinstance(item, str) for item in plan):
        plan_text = "\n".join(plan)
    else:
        try:
            plan_text = json.dumps(plan, indent=2, sort_keys=True, ensure_ascii=False)
        except (TypeError, ValueError):
            plan_text = str(plan)

    lines = plan_text.splitlines()
    if len(lines) <= _MAX_EXPLAIN_LINES:
        return f"```text\n{plan_text}\n```"

    anchor = f"full-plan-{finding_idx}"
    truncated = "\n".join(lines[:_MAX_EXPLAIN_LINES])
    hidden_count = len(lines) - _MAX_EXPLAIN_LINES
    return (
        f"```text\n{truncated}\n... (truncated {hidden_count} lines)\n```\n\n"
        f"[show full plan](#{anchor})\n\n"
        f'<a id="{anchor}"></a>\n'
        "<details><summary>Full EXPLAIN plan</summary>\n\n"
        f"```text\n{plan_text}\n```\n"
        "</details>"
    )


def _render_finding(issue: Issue, finding_idx: int) -> str:
    severity_badges = {
        "CRITICAL": "🔴 CRITICAL",
        "WARNING": "🟡 WARNING",
        "INFO": "ℹ️ INFO",
    }
    badge = severity_badges.get(issue.severity.value, issue.severity.value)
    summary = f"{badge} `{issue.detector_name}` — {issue.description}"

    chunks = [f"<details>\n<summary>{summary}</summary>\n"]
    chunks.append(f"\n- **Suggested action:** {issue.suggested_action}")

    if issue.affected_table:
        chunks.append(f"- **Affected table:** `{issue.affected_table}`")
    if issue.affected_columns:
        formatted_columns = ", ".join(f"`{col}`" for col in issue.affected_columns)
        chunks.append(f"- **Affected columns:** {formatted_columns}")

    for sql in _extract_sql_blocks(issue):
        chunks.append("\n```sql\n" + sql + "\n```")

    context = issue.context or {}
    explain_plan = (
        context.get("explain_plan")
        or context.get("explain")
        or context.get("plan")
        or None
    )
    if explain_plan is not None:
        chunks.append("\n" + _render_explain_plan(explain_plan, finding_idx))

    chunks.append("\n</details>")
    return "\n".join(chunks)


def generate_pr_comment(result: AnalysisResult, *, now: datetime | None = None) -> str:
    issues = result.issues
    header = f"## pgreviewer — {_risk_badge(issues)}"
    body: list[str] = [REPORT_SIGNATURE, header, ""]
    finding_idx = 0
    for section in build_report_sections(result):
        body.append(f"### {section.title}")
        if not section.findings:
            body.append("_No findings._")
            body.append("")
            continue

        if section.section_type == SectionType.SUMMARY:
            for finding in section.findings:
                body.append(f"- **{finding.title}:** {finding.detail}")
            body.append("")
            continue

        if section.section_type == SectionType.QUERY_PERFORMANCE:
            current_file: str | None = None
            current_severity: str | None = None
            for finding in section.findings:
                source_file = finding.source_file or "unknown"
                if source_file != current_file:
                    current_file = source_file
                    current_severity = None
                    body.append(f"#### `{source_file}`")
                severity_label = (
                    finding.severity.value if finding.severity is not None else "INFO"
                )
                if severity_label != current_severity:
                    current_severity = severity_label
                    body.append(f"##### {severity_label}")
                if finding.issue is None:
                    continue
                finding_idx += 1
                body.append(_render_finding(finding.issue, finding_idx))
                body.append("")
            continue

        if section.section_type == SectionType.INDEX_RECOMMENDATIONS:
            for finding in section.findings:
                recommendation = finding.recommendation
                if recommendation is None:
                    continue
                body.append(
                    "- "
                    f"`{finding.title}` "
                    f"(improvement: {recommendation.improvement_pct * 100:.1f}%, "
                    f"validated: {'yes' if recommendation.validated else 'no'})"
                )
            body.append("")
            continue

        for finding in section.findings:
            if finding.issue is None:
                continue
            finding_idx += 1
            body.append(_render_finding(finding.issue, finding_idx))
            body.append("")

    body.append("---")
    body.append(
        f'*pgreviewer · <a href="{_DOCS_URL}">docs</a> · suppress with '
        "`-- pgreviewer:ignore`*"
    )
    last_updated = (now or datetime.now(UTC)).strftime("%Y-%m-%d %H:%M UTC")
    body.append(f"*Last updated: {last_updated}*")
    return "\n".join(body)
