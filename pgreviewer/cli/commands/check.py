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
_RECOMMENDATION_NORMAL_CONFIDENCE = 0.85
_RECOMMENDATION_MODERATE_CONFIDENCE = 0.70


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

    high_and_moderate = [
        rec for rec in recs if rec.confidence >= _RECOMMENDATION_MODERATE_CONFIDENCE
    ]
    low_confidence = [
        rec for rec in recs if rec.confidence < _RECOMMENDATION_MODERATE_CONFIDENCE
    ]

    if high_and_moderate:
        console.rule("[bold]Recommended Indexes[/bold]")
    for rec in high_and_moderate:
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
        if (
            _RECOMMENDATION_MODERATE_CONFIDENCE
            <= rec.confidence
            < _RECOMMENDATION_NORMAL_CONFIDENCE
        ):
            console.print(
                "   [yellow]⚠️  moderate confidence — verify before applying[/yellow]"
            )
        for note in rec.notes:
            console.print(f"   [yellow]⚠ Note: {note}[/yellow]")
        console.print()

    if low_confidence:
        console.rule("[bold]Possible issues (low confidence)[/bold]")
        for rec in low_confidence:
            console.print("[yellow]🔍 manual review recommended[/yellow]")
            console.print(f"   [bold]{rec.create_statement}[/bold]")
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
    from pgreviewer.analysis.index_generator import generate_create_index
    from pgreviewer.analysis.index_suggester import IndexCandidate
    from pgreviewer.analysis.issue_detectors import run_all_detectors
    from pgreviewer.analysis.plan_parser import extract_tables, parse_explain
    from pgreviewer.config import settings
    from pgreviewer.core.backend import get_backend
    from pgreviewer.core.degradation import AnalysisResult
    from pgreviewer.core.models import IndexRecommendation, SchemaInfo
    from pgreviewer.exceptions import (
        BudgetExceededError,
        InvalidQueryError,
        LLMUnavailableError,
        StructuredOutputError,
    )
    from pgreviewer.infra.debug_store import LLM_ROUTING, DebugStore

    result = AnalysisResult()
    backend = get_backend(settings)

    try:
        # 1. Broad analysis (Read-only)
        raw_plan = await backend.get_explain_plan(sql)
        plan = parse_explain(raw_plan)
        tables = extract_tables(plan)
        if tables:
            schema = SchemaInfo(
                tables={
                    table_name: await backend.get_schema_info(table_name)
                    for table_name in tables
                }
            )
        else:
            schema = SchemaInfo()
        issues = run_all_detectors(
            plan, schema, disabled_detectors=settings.DISABLED_DETECTORS
        )

        recommendations = await backend.recommend_indexes([sql])
        recommendations.sort(key=lambda r: r.improvement_pct, reverse=True)
        _detect_redundant_recommendations(recommendations)

        result.issues = issues
        result.recommendations = recommendations

        # 6. LLM interpretation + validation for index suggestions
        if settings.LLM_API_KEY:
            use_llm, route_reason = should_use_llm(plan, issues)
            store = DebugStore(settings.DEBUG_STORE_PATH)
            routing_run_id = store.new_run_id()
            store.save(
                routing_run_id,
                LLM_ROUTING,
                {"use_llm": use_llm, "reason": route_reason},
            )
            if use_llm:
                result.llm_used = True
                try:
                    from pgreviewer.llm.client import LLMClient
                    from pgreviewer.llm.prompts.explain_interpreter import (
                        interpret_explain,
                    )

                    interpretation = interpret_explain(
                        raw_plan,
                        schema,
                        {},
                        client=LLMClient(),
                    )

                    llm_recommendations: list[IndexRecommendation] = []
                    if interpretation.suggested_indexes:
                        baseline_cost = float(raw_plan["Plan"]["Total Cost"])
                        for suggestion in interpretation.suggested_indexes:
                            candidate = IndexCandidate(
                                table=suggestion.table,
                                columns=suggestion.columns,
                                rationale=suggestion.rationale,
                            )
                            rec = IndexRecommendation(
                                table=candidate.table,
                                columns=candidate.columns,
                                index_type=candidate.index_type,
                                is_unique=candidate.is_unique,
                                partial_predicate=candidate.partial_predicate,
                                source="llm",
                                rationale=candidate.rationale,
                                confidence=suggestion.confidence,
                            )
                            rec.create_statement = generate_create_index(rec)

                            try:
                                improved_plan = await backend.get_explain_plan(
                                    sql,
                                    [rec.create_statement],
                                )
                            except InvalidQueryError:
                                # Hard rejection (hallucinated schema objects).
                                suggestion.validated = False
                                continue
                            except Exception as exc:
                                rec.validated = False
                                rec.notes.append(
                                    f"HypoPG validation unavailable: {exc}"
                                )
                                llm_recommendations.append(rec)
                                continue

                            cost_after = float(improved_plan["Plan"]["Total Cost"])
                            if baseline_cost > 0:
                                improvement_pct = (
                                    baseline_cost - cost_after
                                ) / baseline_cost
                            else:
                                improvement_pct = 0.0
                            validated = (
                                improvement_pct >= settings.HYPOPG_MIN_IMPROVEMENT
                            )
                            suggestion.validated = validated
                            suggestion.cost_before = baseline_cost
                            suggestion.cost_after = cost_after
                            suggestion.improvement_pct = improvement_pct
                            if not validated:
                                continue

                            rec.cost_before = baseline_cost
                            rec.cost_after = cost_after
                            rec.improvement_pct = improvement_pct
                            rec.validated = True
                            rec.source = "llm+hypopg"
                            llm_recommendations.append(rec)

                    if llm_recommendations:
                        result.recommendations.extend(llm_recommendations)
                        result.recommendations.sort(
                            key=lambda r: r.improvement_pct, reverse=True
                        )
                        _detect_redundant_recommendations(result.recommendations)
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
        if settings.BACKEND.lower() in {"local", "hybrid"}:
            from pgreviewer.db.pool import close_pool

            await close_pool()
