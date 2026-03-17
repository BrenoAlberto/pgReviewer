"""Implementation of the ``pgr workload`` command."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import typer
from rich.console import Console
from rich.table import Table

from pgreviewer.cli.commands.check import _analyse_query

if TYPE_CHECKING:
    from pgreviewer.core.degradation import AnalysisResult
    from pgreviewer.core.models import SlowQuery

console = Console()
err_console = Console(stderr=True)

_FINGERPRINT_MAX_LEN = 70
_RECOMMENDATION_MAX_LEN = 72


@dataclass
class WorkloadQueryAnalysis:
    query_fingerprint: str
    calls_per_day: int
    avg_time_ms: float
    issues_found: int
    top_recommendation: str


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _query_fingerprint(query: str) -> str:
    one_line = " ".join(query.split())
    return _truncate(one_line, _FINGERPRINT_MAX_LEN)


def _top_recommendation(result: AnalysisResult) -> str:
    if not result.recommendations:
        return "—"
    statement = " ".join(result.recommendations[0].create_statement.split())
    return _truncate(statement, _RECOMMENDATION_MAX_LEN)


async def _fetch_slow_queries(limit: int) -> list[SlowQuery]:
    from pgreviewer.config import settings
    from pgreviewer.core.backend import get_backend

    return await get_backend(settings).get_slow_queries(limit=limit)


async def _analyze_slow_queries(
    slow_queries: list[SlowQuery],
) -> list[WorkloadQueryAnalysis]:
    results: list[WorkloadQueryAnalysis] = []
    for slow_query in slow_queries:
        try:
            analysis = await _analyse_query(slow_query.query_text)
            issues_found = len(analysis.issues)
            top_recommendation = _top_recommendation(analysis)
        except Exception:
            issues_found = 0
            top_recommendation = "analysis failed"
        results.append(
            WorkloadQueryAnalysis(
                query_fingerprint=_query_fingerprint(slow_query.query_text),
                calls_per_day=slow_query.calls,
                avg_time_ms=slow_query.mean_exec_time_ms,
                issues_found=issues_found,
                top_recommendation=top_recommendation,
            )
        )
    return results


def _render_rich_table(rows: list[WorkloadQueryAnalysis]) -> None:
    table = Table(show_header=True, header_style="bold cyan", expand=True)
    table.add_column("Rank", width=6)
    table.add_column("Query fingerprint")
    table.add_column("Calls/day", justify="right")
    table.add_column("Avg time (ms)", justify="right")
    table.add_column("Issues found", justify="right")
    table.add_column("Top recommendation")

    for rank, row in enumerate(rows, start=1):
        table.add_row(
            str(rank),
            row.query_fingerprint,
            f"{row.calls_per_day:,}",
            f"{row.avg_time_ms:.2f}",
            str(row.issues_found),
            row.top_recommendation,
        )
    console.print(table)


def _escape_markdown_cell(value: str) -> str:
    return value.replace("|", "\\|")


def _render_markdown_table(rows: list[WorkloadQueryAnalysis]) -> str:
    header = (
        "| Rank | Query fingerprint | Calls/day | Avg time (ms) | Issues found | "
        "Top recommendation |"
    )
    lines = [
        header,
        "| ---: | --- | ---: | ---: | ---: | --- |",
    ]
    for rank, row in enumerate(rows, start=1):
        lines.append(
            "| "
            f"{rank} | "
            f"{_escape_markdown_cell(row.query_fingerprint)} | "
            f"{row.calls_per_day:,} | "
            f"{row.avg_time_ms:.2f} | "
            f"{row.issues_found} | "
            f"{_escape_markdown_cell(row.top_recommendation)} |"
        )
    return "\n".join(lines)


def run_workload(top: int, min_calls: int, export: Literal["markdown"] | None) -> None:
    try:
        slow_queries = asyncio.run(_fetch_slow_queries(limit=top))
    except Exception as exc:
        err_console.print(f"[red]Error:[/red] Unable to fetch slow queries: {exc}")
        raise typer.Exit(code=1) from None

    filtered_queries = [query for query in slow_queries if query.calls > min_calls]
    if not filtered_queries:
        console.print("No slow queries matched the selected filters.")
        return

    rows = asyncio.run(_analyze_slow_queries(filtered_queries))

    if export == "markdown":
        typer.echo(_render_markdown_table(rows))
        return
    _render_rich_table(rows)
