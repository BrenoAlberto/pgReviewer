"""Implementation of the ``pgr check-models`` command."""

from __future__ import annotations

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


def _overall_severity(issues: list[Issue]) -> str:
    from pgreviewer.core.models import Severity

    if any(i.severity == Severity.CRITICAL for i in issues):
        return "CRITICAL"
    if any(i.severity == Severity.WARNING for i in issues):
        return "WARNING"
    if any(i.severity == Severity.INFO for i in issues):
        return "INFO"
    return "PASS"


def _print_rich_report(path: str, issues: list[Issue]) -> None:
    sev = _overall_severity(issues)
    badge = _SEVERITY_BADGE.get(sev, f"🟢 {sev}") if sev != "PASS" else "🟢 PASS"

    console.rule("[bold]Static Model Analysis[/bold]")
    console.print(f"[bold]Path:[/bold] {path}")
    style = _SEVERITY_STYLE.get(sev, "green")
    console.print(f"[bold]Overall:[/bold] [{style}]{badge}[/{style}]")
    console.print()

    if issues:
        table = Table(show_header=True, header_style="bold cyan", expand=True)
        table.add_column("Severity", style="bold", width=10)
        table.add_column("Detector", width=24)
        table.add_column("Table/Class", width=20)
        table.add_column("Description")
        table.add_column("Suggested Action")

        for issue in issues:
            row_style = _SEVERITY_STYLE.get(issue.severity.value, "")
            sev_label = _SEVERITY_BADGE.get(issue.severity.value, issue.severity.value)
            table.add_row(
                f"[{row_style}]{sev_label}[/{row_style}]",
                issue.detector_name,
                issue.affected_table or "-",
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
    console.print()


def _print_json_report(path: str, issues: list[Issue]) -> None:
    output = {
        "path": path,
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
                "file": i.context.get("file"),
            }
            for i in issues
        ],
    }
    sys.stdout.write(json.dumps(output, indent=2) + "\n")


def run_check_models(
    path: Path,
    fix: bool = False,
    json_output: bool = False,
) -> None:
    """Core logic for the ``pgr check-models`` command."""
    from pgreviewer.analysis.static_model_checker import check_models_in_path

    if not path.exists():
        err_console.print(f"[red]Error:[/red] Path '{path}' does not exist.")
        raise typer.Exit(code=1)

    try:
        issues = check_models_in_path(path)
    except Exception as exc:
        err_console.print(f"[red]Error:[/red] {exc}")
        if "--debug" in sys.argv:
            import traceback

            traceback.print_exc()
        raise typer.Exit(code=1) from exc

    if fix:
        console.print(
            "[yellow]Notice: --fix is a stub and currently only "
            "reports what to change.[/yellow]"
        )
        console.print()

    if json_output:
        _print_json_report(str(path), issues)
    else:
        _print_rich_report(str(path), issues)
