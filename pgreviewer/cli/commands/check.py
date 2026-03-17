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

    from pgreviewer.core.degradation import AnalysisResult
    from pgreviewer.core.models import IndexRecommendation, Issue

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


def _print_rich_report(sql: str, result: AnalysisResult) -> None:
    issues = result.issues
    sev = _overall_severity(issues)
    badge = _SEVERITY_BADGE.get(sev, f"🟢 {sev}") if sev != "PASS" else "🟢 PASS"

    console.rule("[bold]pgReviewer Analysis[/bold]")
    if result.llm_degraded:
        msg = result.degradation_reason or "LLM analysis unavailable"
        console.print(f"[yellow]⚠️  {msg} — showing algorithmic analysis only[/yellow]")
        console.print()
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
    console.print()


def _detect_redundant_recommendations(recs: list[IndexRecommendation]) -> None:
    """Flag recommendations whose columns are a strict subset of another's.

    If candidate B's columns are a strict subset of candidate A's columns on
    the same table, B is annotated as potentially redundant (the composite
    index A would cover B's use-case as well).
    """
    for i, rec_b in enumerate(recs):
        b_cols = set(rec_b.columns)
        for j, rec_a in enumerate(recs):
            if i == j:
                continue
            if rec_a.table != rec_b.table:
                continue
            a_cols = set(rec_a.columns)
            if b_cols < a_cols:  # strict subset
                rec_b.notes.append(
                    f"Potentially redundant: columns are a subset of the "
                    f"composite index on ({', '.join(rec_a.columns)})"
                )
                break  # one note is enough


def _print_recommendations(recs: list[IndexRecommendation]) -> None:
    if not recs:
        return

    console.rule("[bold]Recommended Indexes[/bold]")
    for rec in recs:
        if rec.validated:
            title = "💡 Suggested index (HypoPG validated ✓)"
            style = "green"
        else:
            title = "⚠️  Suggested index (Not validated)"
            style = "yellow"

        console.print(f"[{style}]{title}[/{style}]")
        console.print(f"   [bold]{rec.create_statement}[/bold]")
        pc = rec.improvement_pct * 100
        console.print(
            f"   Cost: {rec.cost_before:.2f} → {rec.cost_after:.2f}  "
            f"(improvement: {pc:.1f}%)"
        )
        if rec.rationale:
            console.print(f"   [dim]Rationale: {rec.rationale}[/dim]")
        for note in rec.notes:
            console.print(f"   [yellow]⚠ Note: {note}[/yellow]")
        console.print()

    if len(recs) > 3:
        console.print(
            "[yellow]⚠ Adding more indexes may have diminishing "
            "write-performance returns. Profile before applying all.[/yellow]"
        )
        console.print()


def _print_json_report(sql: str, result: AnalysisResult) -> None:
    issues = result.issues
    recs = result.recommendations
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
        "recommendations": [r.to_dict() for r in recs],
        "llm_used": result.llm_used,
        "llm_degraded": result.llm_degraded,
        "degradation_reason": result.degradation_reason,
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
    try:
        result = asyncio.run(_analyse_query(sql))
    except Exception as exc:
        err_console.print(f"[red]Error:[/red] {exc}")
        if "--debug" in sys.argv:
            import traceback

            traceback.print_exc()
        raise typer.Exit(code=1) from None

    if json_output:
        _print_json_report(sql, result)
    else:
        _print_rich_report(sql, result)
        _print_recommendations(result.recommendations)


async def _analyse_query(sql: str) -> AnalysisResult:
    """Internal analysis pipeline."""
    from pgreviewer.analysis.complexity_router import should_use_llm
    from pgreviewer.analysis.explain_runner import run_explain
    from pgreviewer.analysis.hypopg_validator import (
        validate_candidate,
        validate_candidates_combined,
    )
    from pgreviewer.analysis.index_generator import generate_create_index
    from pgreviewer.analysis.index_suggester import suggest_indexes
    from pgreviewer.analysis.issue_detectors import run_all_detectors
    from pgreviewer.analysis.plan_parser import extract_tables, parse_explain
    from pgreviewer.analysis.schema_collector import collect_schema
    from pgreviewer.config import settings
    from pgreviewer.core.degradation import AnalysisResult
    from pgreviewer.core.models import IndexRecommendation, SchemaInfo
    from pgreviewer.db.pool import close_pool, read_session, write_session
    from pgreviewer.exceptions import (
        BudgetExceededError,
        LLMUnavailableError,
        StructuredOutputError,
    )
    from pgreviewer.infra.debug_store import LLM_ROUTING, DebugStore

    result = AnalysisResult()

    try:
        # 1. Broad analysis (Read-only)
        async with read_session() as conn:
            raw_plan = await run_explain(sql, conn)
            plan = parse_explain(raw_plan)
            tables = extract_tables(plan)
            schema = await collect_schema(tables, conn) if tables else SchemaInfo()
            issues = run_all_detectors(
                plan, schema, disabled_detectors=settings.DISABLED_DETECTORS
            )

        # 2. Index candidates
        candidates = suggest_indexes(issues, schema)
        recommendations = []

        if candidates:
            # 3. Validation (Write session for HypoPG, but always rolls back)
            async with write_session() as conn:
                # Validate each candidate independently (for per-index improvement_pct)
                for cand in candidates:
                    v_res = await validate_candidate(cand, sql, conn)

                    rec = IndexRecommendation(
                        table=cand.table,
                        columns=cand.columns,
                        index_type=cand.index_type,
                        is_unique=cand.is_unique,
                        partial_predicate=cand.partial_predicate,
                        cost_before=v_res.cost_before,
                        cost_after=v_res.cost_after,
                        improvement_pct=v_res.improvement_pct,
                        validated=v_res.validated,
                        rationale=v_res.rationale or cand.rationale,
                    )
                    # Generate the copyable statement
                    rec.create_statement = generate_create_index(rec)
                    recommendations.append(rec)

                # Also validate all candidates simultaneously to capture the
                # true combined improvement (which may differ from individual sums)
                await validate_candidates_combined(candidates, sql, conn)

            # 4. Rank by individual improvement_pct descending
            recommendations.sort(key=lambda r: r.improvement_pct, reverse=True)

            # 5. Flag recommendations whose columns are a subset of another's
            _detect_redundant_recommendations(recommendations)

        result.issues = issues
        result.recommendations = recommendations

        # 6. LLM Interpretation (Placeholder for actual implementation)
        # Wrap LLM call sites to support graceful degradation
        if settings.LLM_API_KEY:
            use_llm, route_reason = should_use_llm(plan, issues)
            store = DebugStore(settings.DEBUG_STORE_PATH)
            store.save(
                store.new_run_id(),
                LLM_ROUTING,
                {"use_llm": use_llm, "reason": route_reason},
            )
            if use_llm:
                result.llm_used = True
                try:
                    from pgreviewer.llm.client import LLMClient

                    # We call this to provide a hook for degradation testing
                    LLMClient().generate(
                        f"Interpret this SQL plan: {sql}",
                        category="interpretation",
                        estimated_tokens=500,
                    )
                except (
                    LLMUnavailableError,
                    BudgetExceededError,
                    StructuredOutputError,
                ) as e:
                    import logging

                    logging.getLogger(__name__).warning("LLM analysis degraded: %s", e)
                    result.llm_degraded = True
                    result.degradation_reason = str(e)

        return result
    finally:
        await close_pool()
