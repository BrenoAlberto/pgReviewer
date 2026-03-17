"""Format pgr diff JSON output as a polished GitHub PR comment."""

from __future__ import annotations

import re
from collections import defaultdict
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

_SEV_ORDER = {"CRITICAL": 0, "WARNING": 1, "INFO": 2}

# Human-readable context for known detectors.
# Each entry: (title, why_it_matters, fix_hint)
_DETECTOR_CONTEXT: dict[str, tuple[str, str, str]] = {
    "create_index_not_concurrently": (
        "Missing `CONCURRENTLY` on `CREATE INDEX`",
        "Without `CONCURRENTLY`, Postgres acquires an `AccessExclusiveLock` on the "
        "table for the **entire** index build — blocking all reads and writes. On any "
        "production table with live traffic this causes downtime.",
        "Use `CREATE INDEX CONCURRENTLY`. Because `CONCURRENTLY` cannot run inside a "
        "transaction, wrap it in `op.execute()` and mark the migration "
        "non-transactional, or split it into a separate migration step.",
    ),
    "add_foreign_key_without_index": (
        "Foreign key column missing an index",
        "FK columns without indexes force Postgres to do a full sequential scan on the "
        "referencing table for every join, cascade check, and ON DELETE action. "
        "This becomes severe as the table grows.",
        "Add a `CREATE INDEX` on the FK column in the same migration.",
    ),
    "add_column_with_default": (
        "Adding a column with a non-volatile `DEFAULT`",
        "In Postgres < 11, adding a column with a `DEFAULT` rewrites the entire table, "
        "taking an `AccessExclusiveLock` for minutes on large tables. Postgres 11+ "
        "handles non-volatile defaults efficiently, but volatile ones (e.g. `NOW()`) "
        "still require a rewrite.",
        "For Postgres 11+: adding a non-volatile default is safe. "
        "For volatile defaults or older Postgres: add the column `DEFAULT NULL` first, "
        "backfill in batches, then add the `NOT NULL` constraint.",
    ),
    "destructive_ddl": (
        "Destructive DDL operation",
        "Dropping a table or column is irreversible. Any code that still references "
        "the removed object will error immediately after the migration runs.",
        "Ensure no application code references this object before deploying. "
        "Consider a two-phase approach: deprecate → remove.",
    ),
    "alter_column_type": (
        "Column type change",
        "Changing a column type usually rewrites the entire table, holding an "
        "`AccessExclusiveLock` for the duration and blocking all traffic.",
        "For safe type changes: add a new column, dual-write, backfill, swap, "
        "then drop the old column across multiple deployments.",
    ),
    "large_table_ddl": (
        "DDL on a large table",
        "Certain DDL operations (adding `NOT NULL`, changing constraints) acquire "
        "long-held locks on large tables, causing extended downtime.",
        "Use `NOT VALID` for new constraints, validate separately with lower lock "
        "contention.",
    ),
    "add_not_null_without_default": (
        "Adding `NOT NULL` without a `DEFAULT`",
        "Adding a `NOT NULL` constraint without a default validates every existing row,"
        " requiring a full table scan under `AccessExclusiveLock`.",
        "Add the constraint as `NOT VALID`, backfill nulls, then `VALIDATE CONSTRAINT` "
        "in a separate step (uses `ShareUpdateExclusiveLock`, non-blocking).",
    ),
    "drop_column_still_referenced": (
        "Dropping a column still referenced in application code",
        "Dropping a column while application code still queries it will cause "
        "immediate runtime errors.",
        "Remove all code references first, deploy, then drop the column.",
    ),
}


# ── Helpers ───────────────────────────────────────────────────────────────────

_SQL_AFTER_ACTION_RE = re.compile(
    r"(?:blocking writers|Suggested SQL|suggested fix|replace with)"
    r":\s*(.+?)(?:\.\s*Note:|$)",
    re.IGNORECASE | re.DOTALL,
)


def _extract_fix_sql(suggested_action: str) -> str | None:
    m = _SQL_AFTER_ACTION_RE.search(suggested_action)
    if m:
        sql = m.group(1).strip().rstrip(";") + ";"
        if sql.upper().startswith(
            ("CREATE", "ALTER", "DROP", "INSERT", "UPDATE", "SELECT")
        ):
            return sql
    return None


def _overall_badge(critical: int, warning: int) -> str:
    if critical:
        return "🔴&nbsp;CRITICAL"
    if warning:
        return "🟡&nbsp;WARNING"
    return "🟢&nbsp;PASS"


def _worst_severity(rows: list[dict]) -> str:
    return min(rows, key=lambda r: _SEV_ORDER.get(r["severity"], 99))["severity"]


def _location_links(rows: list[dict]) -> str:
    """Compact comma-separated location list, de-duplicated."""
    seen: set[str] = set()
    parts: list[str] = []
    for row in rows:
        loc = row["location"]
        if loc not in seen:
            seen.add(loc)
            parts.append(loc)
    return ", ".join(parts)


# ── Main formatter ─────────────────────────────────────────────────────────────


def format_diff_comment(data: dict[str, Any], *, now: datetime | None = None) -> str:
    now = now or datetime.now(UTC)
    ts = now.strftime("%Y-%m-%d %H:%M UTC")

    results: list[dict] = data.get("results", [])
    model_diffs: list[dict] = data.get("model_diffs", [])
    skipped: list[dict] = data.get("skipped", [])
    cross: list[dict] = data.get("cross_cutting_findings", [])

    # ── Flatten all issues ────────────────────────────────────────────────────
    rows: list[dict] = []

    for result in results:
        src = result.get("source_file", "unknown")
        line = result.get("line_number", "")
        for issue in result.get("issues", []):
            rows.append(
                {
                    "severity": issue.get("severity", "INFO"),
                    "location": f"`{src}`&nbsp;L{line}" if line else f"`{src}`",
                    "detector": issue.get("detector_name", ""),
                    "description": issue.get("description", ""),
                    "suggested_action": issue.get("suggested_action", ""),
                }
            )

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

    for finding in cross:
        mf = finding.get("migration_source", {})
        line = mf.get("line_number", "")
        src = mf.get("file", "unknown")
        rows.append(
            {
                "severity": finding.get("severity", "INFO"),
                "location": f"`{src}`&nbsp;L{line}" if line else f"`{src}`",
                "detector": finding.get("detector_name", ""),
                "description": finding.get("description", ""),
                "suggested_action": finding.get("suggested_action", ""),
            }
        )

    # ── Counts ────────────────────────────────────────────────────────────────
    critical = sum(1 for r in rows if r["severity"] == "CRITICAL")
    warning = sum(1 for r in rows if r["severity"] == "WARNING")
    info = sum(1 for r in rows if r["severity"] == "INFO")
    total = critical + warning + info

    badge = _overall_badge(critical, warning)

    parts: list[str] = [REPORT_SIGNATURE]

    # ── Header ────────────────────────────────────────────────────────────────
    parts.append(
        f'<p align="center">'
        f'<a href="{_REPO_URL}">'
        f'<img src="{_LOGO_URL}" height="52" alt="pgReviewer" /></a>'
        f"</p>"
    )
    parts.append(f'<h2 align="center">pgReviewer &nbsp;—&nbsp; {badge}</h2>')
    parts.append("")

    if total == 0:
        parts.append(
            '<p align="center">✅&nbsp;No issues found. This migration looks good.</p>'
        )
    else:
        summary_parts = []
        if critical:
            summary_parts.append(f"**{critical} critical**")
        if warning:
            summary_parts.append(f"**{warning} warning**")
        if info:
            summary_parts.append(f"**{info} info**")
        n_files = len(
            {r["location"].split("`")[1] for r in rows if "`" in r["location"]}
        )
        parts.append(
            '<p align="center">'
            + " &nbsp;·&nbsp; ".join(summary_parts)
            + f"&nbsp; {'issue' if total == 1 else 'issues'} across "
            + f"{n_files} {'file' if n_files == 1 else 'files'}"
            + "</p>"
        )

    parts.append("")
    parts.append("---")
    parts.append("")

    # ── Grouped findings ──────────────────────────────────────────────────────
    if rows:
        # Group by detector_name, sort by worst severity first
        groups: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            groups[row["detector"]].append(row)

        sorted_groups = sorted(
            groups.items(),
            key=lambda kv: _SEV_ORDER.get(_worst_severity(kv[1]), 99),
        )

        for detector, instances in sorted_groups:
            worst = _worst_severity(instances)
            icon = _SEV_ICON.get(worst, "ℹ️")
            count = len(instances)
            count_str = f"{count} occurrence{'s' if count > 1 else ''}"

            ctx = _DETECTOR_CONTEXT.get(detector)
            title = ctx[0] if ctx else f"`{detector}`"
            why = ctx[1] if ctx else None
            fix_hint = ctx[2] if ctx else None

            # Section header
            parts.append(f"#### {icon} {title} &nbsp;<sup>{count_str}</sup>")
            parts.append("")

            if why:
                parts.append(f"> {why}")
                parts.append("")

            # Affected locations (compact)
            parts.append(f"**Affected:** {_location_links(instances)}")
            parts.append("")

            # Fix section
            fix_sqls: list[str] = []
            for inst in instances:
                sql = _extract_fix_sql(inst["suggested_action"])
                if sql and sql not in fix_sqls:
                    fix_sqls.append(sql)

            if fix_hint or fix_sqls:
                summary_label = "Fix"
                if fix_sqls:
                    first = fix_sqls[0]
                    preview = first[:60] + ("…" if len(first) > 60 else "")
                    summary_label = f"Fix &nbsp;·&nbsp; `{preview}`"
                parts.append(f"<details><summary><b>{summary_label}</b></summary>")
                parts.append("")
                if fix_hint:
                    parts.append(fix_hint)
                    parts.append("")
                if fix_sqls:
                    parts.append("```sql")
                    for sql in fix_sqls[:3]:  # cap at 3 to avoid bloat
                        parts.append(sql)
                    if len(fix_sqls) > 3:
                        n_more = len(fix_sqls) - 3
                        parts.append(
                            f"-- ... and {n_more} more (see inline review comments)"
                        )
                    parts.append("```")
                parts.append("")
                parts.append("</details>")

            parts.append("")

    # ── Skipped files ─────────────────────────────────────────────────────────
    if skipped:
        parts.append(
            "<details>"
            f"<summary><sub>{len(skipped)} file{'s' if len(skipped) != 1 else ''} "
            "skipped</sub></summary>"
        )
        parts.append("")
        for s in skipped:
            parts.append(f"- `{s['file']}` — {s['reason']}")
        parts.append("")
        parts.append("</details>")
        parts.append("")

    # ── Footer ────────────────────────────────────────────────────────────────
    parts.append("---")
    parts.append("")
    parts.append(
        f"<sub>🤖&nbsp;Generated by "
        f'<a href="{_REPO_URL}"><b>pgReviewer</b></a>'
        f"&nbsp;·&nbsp;{ts}"
        f"&nbsp;·&nbsp;Inline suggestions posted as review comments</sub>"
    )

    return "\n".join(parts)
