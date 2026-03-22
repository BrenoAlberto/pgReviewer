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
        "Use `op.get_context().autocommit_block()` with `postgresql_concurrently=True`"
        " and `if_not_exists=True` (idempotent re-runs). `CONCURRENTLY` cannot run "
        "inside a transaction — wrap in `autocommit_block()` or split into a separate "
        "non-transactional migration step.",
    ),
    "drop_index_not_concurrently": (
        "Missing `CONCURRENTLY` on `DROP INDEX`",
        "Without `CONCURRENTLY`, `DROP INDEX` acquires an `AccessExclusiveLock` for "
        "the entire operation — blocking all reads and writes on the table.",
        "Use `op.drop_index(..., postgresql_concurrently=True, if_exists=True)` inside"
        " `op.get_context().autocommit_block()`. Same transaction restriction as "
        "`CREATE INDEX CONCURRENTLY` applies.",
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
    "missing_fk_index": (
        "Foreign key column missing an index",
        "PostgreSQL does not auto-create indexes on foreign key columns. Every join, "
        "cascade check, and ON DELETE operation on this column will do a full "
        "sequential scan — severe as the table grows.",
        "Add `index=True` to the column definition, or add an explicit "
        "`Index('idx_name', 'column_name')` to the model's `__table_args__`.",
    ),
    "removed_index": (
        "Named index removed from model",
        "Removing an index causes sequential scans on every query that previously "
        "used it. On large tables this can cause immediate and significant latency "
        "regressions.",
        "Verify no active queries rely on this index before removing it. "
        "Consider a staged removal: deprecate first, monitor, then drop.",
    ),
    "large_text_without_constraint": (
        "Unconstrained `Text` / `String` column",
        "Unconstrained text columns have no length limit enforced by the database. "
        "This can lead to unexpectedly large rows, bloated storage, and degraded "
        "query performance on high-traffic tables.",
        "Use `String(n)` with an explicit maximum length, or add a check constraint "
        "if you need unbounded storage for specific columns.",
    ),
    "duplicate_pk_index": (
        "Redundant index duplicates primary key",
        "PostgreSQL automatically creates a unique B-tree index for every primary key. "
        "Adding an explicit index on the same column(s) wastes storage and adds write "
        "overhead without any query-planning benefit.",
        "Remove the redundant `Index(...)` — the primary key index already covers it.",
    ),
    "sql_injection_fstring": (
        "SQL injection — f-string interpolation in `execute()`",
        "SQL built with f-strings or string concatenation passes raw values directly "
        "into the query. An attacker who controls any interpolated value can read, "
        "modify, or delete arbitrary data. Even trusted-source values should use "
        "bound parameters — it also lets pgReviewer analyse the query shape "
        "accurately.",
        "Replace string interpolation with parameterised queries:\n"
        "```python\n"
        "# Bad\n"
        "sql = f\"SELECT ... WHERE col = '{val}'\"\n"
        "db.execute(text(sql))\n\n"
        "# Good\n"
        'db.execute(text("SELECT ... WHERE col = :val"), {"val": val})\n'
        "```",
    ),
    "query_in_loop": (
        "N+1 query pattern — query inside loop",
        "A database query is executed inside a loop. For N items this issues N "
        "round-trips to Postgres. At even modest scale (hundreds of rows) this "
        "dominates request latency and can overwhelm connection pools.",
        "Replace with a single batched query using `IN (...)` or an ORM "
        "`joinedload`/`selectinload`. For SQLAlchemy: replace "
        "`db.query(Model).filter(...).all()` inside a loop with one "
        "`db.query(Model).filter(Model.fk.in_(ids)).all()` before the loop.",
    ),
    "sqlalchemy_n_plus_one": (
        "SQLAlchemy N+1 — lazy relationship accessed in loop",
        "Accessing a lazy-loaded relationship inside a loop triggers one SQL "
        "query per iteration. SQLAlchemy's default `lazy='select'` silently "
        "issues N extra queries.",
        "Use `joinedload` or `selectinload` in the query that fetches the "
        "parent objects, or switch the relationship to `lazy='selectin'`.",
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
    meta: dict = data.get("metadata", {})

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
            line = issue.get("line_number", "")
            rows.append(
                {
                    "severity": issue.get("severity", "INFO"),
                    "location": f"`{src}`&nbsp;L{line}" if line else f"`{src}`",
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

    code_pattern_issues: list[dict] = data.get("code_pattern_issues", [])
    for issue in code_pattern_issues:
        src = issue.get("source_file") or "unknown"
        line = issue.get("line_number", "")
        rows.append(
            {
                "severity": issue.get("severity", "INFO"),
                "location": (f"`{src}`&nbsp;L{line}" if line else f"`{src}`"),
                "detector": issue.get("detector_name", ""),
                "description": issue.get("description", ""),
                "suggested_action": issue.get("suggested_action", ""),
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
    # Analysis mode label (Static vs Full)
    analysis_mode = meta.get("analysis_mode", "")
    if analysis_mode == "static_only":
        mode_label = "<sub><em>Static Analysis</em> · no DB</sub>"
    elif analysis_mode == "full":
        mode_label = "<sub><em>Full Analysis</em> · DB + EXPLAIN</sub>"
    else:
        mode_label = ""

    parts.append(f'<h2 align="center">pgReviewer &nbsp;—&nbsp; {badge}</h2>')
    if mode_label:
        parts.append(f'<p align="center">{mode_label}</p>')
    parts.append("")

    if total == 0:
        parts.append(
            '<p align="center">✅&nbsp;No issues found. This migration looks good.</p>'
        )
    else:
        summary_parts = []
        if critical:
            summary_parts.append(f"<strong>{critical} critical</strong>")
        if warning:
            summary_parts.append(f"<strong>{warning} warning</strong>")
        if info:
            summary_parts.append(f"<strong>{info} info</strong>")
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

    # ── Analysis metadata ─────────────────────────────────────────────────────
    if meta:
        meta_parts: list[str] = []
        if meta.get("llm_used"):
            model = str(meta.get("llm_model") or "").replace("`", "")
            model_str = f"&nbsp;`{model}`" if model else ""
            label = "LLM&nbsp;(degraded)" if meta.get("llm_degraded") else "LLM"
            meta_parts.append(f"🧠&nbsp;{label}{model_str}")
        else:
            meta_parts.append("🔢&nbsp;Algorithmic only")
        if meta.get("hypopg_validated"):
            meta_parts.append("🗂️&nbsp;HypoPG validated")
        if meta.get("mcp_used"):
            meta_parts.append("🔌&nbsp;MCP backend")
        llm_cost = meta.get("llm_cost_usd")
        if llm_cost:
            meta_parts.append(f"💰&nbsp;${llm_cost:.4f}&nbsp;spent")
        n_analyzed = meta.get("queries_analyzed", 0)
        n_skipped = meta.get("files_skipped", 0)
        if n_analyzed or n_skipped:
            meta_parts.append(
                f"📄&nbsp;{n_analyzed}&nbsp;quer{'y' if n_analyzed == 1 else 'ies'}"
                f",&nbsp;{n_skipped}&nbsp;skipped"
            )
        parts.append(
            "<details><summary><sub>Analysis details</sub></summary>\n\n"
            + " &nbsp;·&nbsp; ".join(meta_parts)
            + "\n\n</details>"
        )
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
