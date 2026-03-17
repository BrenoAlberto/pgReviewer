from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pgreviewer.core.degradation import AnalysisResult
    from pgreviewer.core.models import Issue

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

_SQL_PREFIX_RE = re.compile(
    r"^\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|WITH)\b", re.IGNORECASE
)

_DOCS_URL = "https://github.com/BrenoAlberto/pgReviewer#readme"
_MAX_EXPLAIN_LINES = 50


def _risk_badge(issues: list[Issue]) -> str:
    critical = sum(1 for i in issues if i.severity.value == "CRITICAL")
    warning = sum(1 for i in issues if i.severity.value == "WARNING")
    info = sum(1 for i in issues if i.severity.value == "INFO")

    parts: list[str] = []
    if critical:
        suffix = "s" if critical != 1 else ""
        parts.append(f"🔴 {critical} critical{suffix}")
    if warning:
        suffix = "s" if warning != 1 else ""
        parts.append(f"🟡 {warning} warning{suffix}")
    if info and not parts:
        suffix = "s" if info != 1 else ""
        parts.append(f"ℹ️ {info} info{suffix}")

    return ", ".join(parts) if parts else "✅ No issues found"


def _category_for(issue: Issue) -> str:
    if issue.detector_name.startswith("cross_cutting_"):
        return "Cross-cutting Findings"
    if issue.detector_name in _MIGRATION_DETECTORS:
        return "Migration Safety"
    return "Query Performance"


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
        plan_text = json.dumps(plan, indent=2, sort_keys=True, ensure_ascii=False)

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
        chunks.append(
            "- **Affected columns:** " + formatted_columns
        )

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


def generate_pr_comment(result: AnalysisResult) -> str:
    issues = result.issues
    header = f"## pgreviewer — {_risk_badge(issues)}"

    if not issues:
        return (
            f"{header}\n\n"
            "---\n"
            f"*pgreviewer · <a href=\"{_DOCS_URL}\">docs</a> · suppress with "
            "`-- pgreviewer:ignore`*"
        )

    grouped = {
        "Query Performance": [],
        "Migration Safety": [],
        "Cross-cutting Findings": [],
    }
    for issue in issues:
        grouped[_category_for(issue)].append(issue)

    body: list[str] = [header, ""]
    finding_idx = 0
    for section_name in (
        "Query Performance",
        "Migration Safety",
        "Cross-cutting Findings",
    ):
        body.append(f"### {section_name}")
        section_issues = grouped[section_name]
        if not section_issues:
            body.append("_No findings._")
            body.append("")
            continue

        for issue in section_issues:
            finding_idx += 1
            body.append(_render_finding(issue, finding_idx))
            body.append("")

    body.append("---")
    body.append(
        f"*pgreviewer · <a href=\"{_DOCS_URL}\">docs</a> · suppress with "
        "`-- pgreviewer:ignore`*"
    )
    return "\n".join(body)
