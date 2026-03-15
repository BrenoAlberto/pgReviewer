"""Implementation of the ``pgr check`` command."""

from __future__ import annotations

import asyncio
import json
import sys
from typing import TYPE_CHECKING

import typer
from rich.console import Console
from rich.table import Table

if TYPE_CHECKING:
    from pathlib import Path

    from pgreviewer.core.models import Issue

console = Console()
err_console = Console(stderr=True)

_SEVERITY_BADGE: dict[str, str] = {
    "CRITICAL": "🔴 CRITICAL",
    "WARNING": "🟡 WARNING",
    "INFO": "ℹ️  INFO",
}

_SEVERITY_STYLE: dict[str, str] = {
    "CRITICAL": "bold red",
    "WARNING": "bold yellow",
    "INFO": "dim",
}

_MAX_QUERY_LEN = 80


def _truncate(text: str, max_len: int = _MAX_QUERY_LEN) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _overall_severity(issues: list[Issue]) -> str:
    from pgreviewer.core.models import Severity

    if any(i.severity == Severity.CRITICAL for i in issues):
        return "CRITICAL"
    if any(i.severity == Severity.WARNING for i in issues):
        return "WARNING"
    return "PASS"


def _print_rich_report(sql: str, issues: list[Issue]) -> None:
    sev = _overall_severity(issues)
    badge = _SEVERITY_BADGE.get(sev, f"🟢 {sev}") if sev != "PASS" else "🟢 PASS"

    console.rule("[bold]pgReviewer Analysis[/bold]")
    console.print(f"[bold]Query:[/bold] {_truncate(sql)}")
    style = _SEVERITY_STYLE.get(sev, "green")
    console.print(f"[bold]Overall:[/bold] [{style}]{badge}[/{style}]")
    console.print()

    if issues:
        table = Table(show_header=True, header_style="bold cyan", expand=True)
        table.add_column("Severity", style="bold", width=10)
        table.add_column("Detector", width=28)
        table.add_column("Description")
        table.add_column("Suggested Action")

        for issue in issues:
            row_style = _SEVERITY_STYLE.get(issue.severity.value, "")
            sev_label = _SEVERITY_BADGE.get(issue.severity.value, issue.severity.value)
            table.add_row(
                f"[{row_style}]{sev_label}[/{row_style}]",
                issue.detector_name,
                issue.description,
                issue.suggested_action,
            )

        console.print(table)
        console.print()

    n_critical = sum(1 for i in issues if i.severity.value == "CRITICAL")
    n_warning = sum(1 for i in issues if i.severity.value == "WARNING")
    total = len(issues)
    w_plural = "s" if n_warning != 1 else ""
    i_plural = "s" if total != 1 else ""
    console.print(
        f"[bold]{total} issue{i_plural} found "
        f"({n_critical} critical, {n_warning} warning{w_plural})[/bold]"
    )


def _print_json_report(sql: str, issues: list[Issue]) -> None:
    output = {
        "query": sql,
        "overall_severity": _overall_severity(issues),
        "issue_count": len(issues),
        "issues": [
            {
                "severity": i.severity.value,
                "detector_name": i.detector_name,
                "description": i.description,
                "affected_table": i.affected_table,
                "affected_columns": i.affected_columns,
                "suggested_action": i.suggested_action,
                "confidence": i.confidence,
            }
            for i in issues
        ],
    }
    sys.stdout.write(json.dumps(output, indent=2) + "\n")


def run_check(
    query: str | None,
    query_file: Path | None,
    json_output: bool,
) -> None:
    """Core logic for the ``pgr check`` command."""

    # --- Resolve the SQL string -------------------------------------------
    if query_file is not None:
        sql = query_file.read_text().strip()
    elif query is not None:
        sql = query.strip()
    else:
        err_console.print("[red]Error:[/red] Provide a SQL query or --query-file.")
        raise typer.Exit(code=1)

    if not sql:
        err_console.print("[red]Error:[/red] SQL query must not be empty.")
        raise typer.Exit(code=1)

    # --- Run the async analysis pipeline ----------------------------------
    async def _analyse() -> list[Issue]:
        from pgreviewer.analysis.explain_runner import run_explain
        from pgreviewer.analysis.issue_detectors import run_all_detectors
        from pgreviewer.analysis.plan_parser import extract_tables, parse_explain
        from pgreviewer.analysis.schema_collector import collect_schema
        from pgreviewer.config import settings
        from pgreviewer.core.models import SchemaInfo
        from pgreviewer.db.pool import close_pool, read_session

        try:
            async with read_session() as conn:
                raw_plan = await run_explain(sql, conn)
                plan = parse_explain(raw_plan)
                tables = extract_tables(plan)
                if tables:
                    schema = await collect_schema(tables, conn)
                else:
                    schema = SchemaInfo()
                return run_all_detectors(
                    plan,
                    schema,
                    disabled_detectors=settings.DISABLED_DETECTORS,
                )
        finally:
            await close_pool()

    try:
        issues = asyncio.run(_analyse())
    except Exception as exc:
        err_console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(code=1) from None

    if json_output:
        _print_json_report(sql, issues)
    else:
        _print_rich_report(sql, issues)
