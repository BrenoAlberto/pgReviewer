"""Format pgr diff JSON output as a polished GitHub PR comment."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from pgreviewer.reporting.pr_comment import REPORT_SIGNATURE

_LOGO_URL = (
    "https://raw.githubusercontent.com/BrenoAlberto/pgReviewer"
    "/main/docs/assets/logo.svg"
)
_REPO_URL = "https://github.com/BrenoAlberto/pgReviewer"

_SEV_ICON = {
    "CRITICAL": "🔴",
    "WARNING": "🟡",
    "INFO": "ℹ️",
}

_SQL_RE = re.compile(r"Suggested SQL:\s*(.+)", re.IGNORECASE | re.DOTALL)


def _extract_sql(suggested_action: str) -> str | None:
    m = _SQL_RE.search(suggested_action)
    return m.group(1).strip().rstrip(";") + ";" if m else None


def _overall_badge(critical: int, warning: int) -> str:
    if critical:
        return "🔴&nbsp;CRITICAL"
    if warning:
        return "🟡&nbsp;WARNING"
    return "🟢&nbsp;PASS"


def _severity_pill(severity: str) -> str:
    icon = _SEV_ICON.get(severity, "ℹ️")
    return f"{icon}&nbsp;{severity}"


def format_diff_comment(data: dict[str, Any], *, now: datetime | None = None) -> str:
    now = now or datetime.now(UTC)
    ts = now.strftime("%Y-%m-%d %H:%M UTC")

    results: list[dict] = data.get("results", [])
    model_diffs: list[dict] = data.get("model_diffs", [])
    skipped: list[dict] = data.get("skipped", [])
    cross: list[dict] = data.get("cross_cutting_findings", [])

    # Flatten all issues into rows
    rows: list[dict] = []
    sql_fixes: list[str] = []

    for result in results:
        src = result.get("source_file", "unknown")
        line = result.get("line_number", "")
        for issue in result.get("issues", []):
            rows.append(
                {
                    "severity": issue.get("severity", "INFO"),
                    "location": f"`{src}`&nbsp;L{line}",
                    "detector": issue.get("detector_name", ""),
                    "description": issue.get("description", ""),
                    "suggested_action": issue.get("suggested_action", ""),
                }
            )
            sql = _extract_sql(issue.get("suggested_action", ""))
            if sql and sql not in sql_fixes:
                sql_fixes.append(sql)

    for entry in model_diffs:
        src = entry.get("file", "unknown")
        for issue in entry.get("model_issues", []):
            rows.append(
                {
                    "severity": issue.get("severity", "INFO"),
                    "location": f"`{src}`",
                    "detector": issue.get("detector_name", ""),
                    "description": issue.get("description", ""),
                    "suggested_action": issue.get("suggested_action", ""),
                }
            )
            sql = _extract_sql(issue.get("suggested_action", ""))
            if sql and sql not in sql_fixes:
                sql_fixes.append(sql)

    for finding in cross:
        sev = finding.get("severity", "INFO")
        mf = finding.get("migration_source", {})
        rows.append(
            {
                "severity": sev,
                "location": (
                    f"`{mf.get('file', 'unknown')}`&nbsp;L{mf.get('line_number', '')}"
                ),
                "detector": finding.get("detector_name", ""),
                "description": finding.get("description", ""),
                "suggested_action": finding.get("suggested_action", ""),
            }
        )

    critical = sum(1 for r in rows if r["severity"] == "CRITICAL")
    warning = sum(1 for r in rows if r["severity"] == "WARNING")
    info = sum(1 for r in rows if r["severity"] == "INFO")
    total = critical + warning + info

    badge = _overall_badge(critical, warning)

    parts: list[str] = [REPORT_SIGNATURE]

    # ── Header ──────────────────────────────────────────────────────────────
    parts.append(
        f'<p align="center">'
        f'<a href="{_REPO_URL}">'
        f'<img src="{_LOGO_URL}" height="52" alt="pgReviewer" /></a>'
        f"</p>"
    )
    parts.append("")
    parts.append(f'<h2 align="center">pgReviewer &nbsp;—&nbsp; {badge}</h2>')
    parts.append("")

    if total == 0:
        parts.append(
            '<p align="center">✅&nbsp;No issues found. This migration looks good.</p>'
        )
        parts.append("")
    else:
        summary_parts = []
        if critical:
            summary_parts.append(f"**{critical} critical**")
        if warning:
            summary_parts.append(f"**{warning} warning**")
        if info:
            summary_parts.append(f"**{info} info**")
        n_files = len(results) + len(model_diffs)
        issue_word = "issue" if total == 1 else "issues"
        file_word = "file" if n_files == 1 else "files"
        parts.append(
            '<p align="center">'
            + " &nbsp;·&nbsp; ".join(summary_parts)
            + f"&nbsp; {issue_word} found across {n_files} analyzed {file_word}"
            + "</p>"
        )
        parts.append("")

    parts.append("---")
    parts.append("")

    # ── Issues table ─────────────────────────────────────────────────────────
    if rows:
        parts.append("### Issues")
        parts.append("")
        parts.append("| &nbsp; | Location | Detector | Description |")
        parts.append("|:---:|---|---|---|")

        for row in rows:
            pill = _severity_pill(row["severity"])
            location = row["location"]
            detector = f"`{row['detector']}`"
            desc = row["description"].replace("|", "\\|").replace("\n", " ")
            parts.append(f"| {pill} | {location} | {detector} | {desc} |")

        parts.append("")

    # ── Copy-ready SQL ───────────────────────────────────────────────────────
    if sql_fixes:
        n_stmts = len(sql_fixes)
        stmt_word = "statement" if n_stmts == 1 else "statements"
        parts.append(
            "<details>"
            "<summary><b>📋&nbsp;Copy-ready fixes</b> "
            f"<code>{n_stmts} {stmt_word}</code>"
            "</summary>"
        )
        parts.append("")
        parts.append("```sql")
        parts.append("-- Generated by pgReviewer — add to your next migration")
        for sql in sql_fixes:
            parts.append(sql)
        parts.append("```")
        parts.append("")
        parts.append("</details>")
        parts.append("")

    # ── Skipped files ────────────────────────────────────────────────────────
    if skipped:
        parts.append(
            "<details>"
            "<summary><sub>Analysis scope — "
            f"{len(skipped)} file{'s' if len(skipped) != 1 else ''} skipped</sub>"
            "</summary>"
        )
        parts.append("")
        for s in skipped:
            parts.append(f"- `{s['file']}` — {s['reason']}")
        parts.append("")
        parts.append("</details>")
        parts.append("")

    # ── Footer ───────────────────────────────────────────────────────────────
    parts.append("---")
    parts.append("")
    parts.append(
        f"<sub>🤖&nbsp;Generated by "
        f'<a href="{_REPO_URL}"><b>pgReviewer</b></a>'
        f"&nbsp;·&nbsp;{ts}</sub>"
    )

    return "\n".join(parts)
