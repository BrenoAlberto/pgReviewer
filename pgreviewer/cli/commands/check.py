"""Implementation of the ``pgr check`` command."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import sys
from typing import TYPE_CHECKING, Any

import typer
from rich.console import Console, Group
from rich.panel import Panel
from rich.syntax import Syntax

if TYPE_CHECKING:
    from pathlib import Path

    from pgreviewer.config import RuntimeConfig
    from pgreviewer.core.degradation import AnalysisResult
    from pgreviewer.core.models import IndexRecommendation, Issue, SlowQuery

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
_MAX_SUGGESTED_ACTION_LEN = 72
_RECOMMENDATION_NORMAL_CONFIDENCE = 0.85
_RECOMMENDATION_MODERATE_CONFIDENCE = 0.70
_DDL_PREFIX_RE = re.compile(
    r"^\s*(CREATE|ALTER|DROP|TRUNCATE|COMMENT|RENAME|GRANT|REVOKE)\b",
    re.IGNORECASE,
)
_CLAUSE_STOP_RE = (
    r"\b(?:group\s+by|order\s+by|limit|offset|having|union|intersect|except)\b"
)
_FROM_OR_JOIN_ALIAS_RE = re.compile(
    r"\b(?:from|join)\s+([a-z_][a-z0-9_\.]*)\s*(?:as\s+)?([a-z_][a-z0-9_]*)?",
    re.IGNORECASE,
)
_WHERE_RE = re.compile(
    rf"\bwhere\b(.*?)(?={_CLAUSE_STOP_RE}|$)", re.IGNORECASE | re.DOTALL
)
_JOIN_ON_RE = re.compile(
    r"\bjoin\b\s+[a-z_][a-z0-9_\.]*\s*(?:(?:as\s+)?[a-z_][a-z0-9_]*)?\s+\bon\b(.*?)(?=\bjoin\b|"
    rf"{_CLAUSE_STOP_RE}|$)",
    re.IGNORECASE | re.DOTALL,
)
_STRING_LITERAL_RE = re.compile(r"'(?:''|[^'])*'")
_NUMERIC_LITERAL_RE = re.compile(r"(?<!\$)\b\d+(?:\.\d+)?\b")
_NOW_LITERAL_RE = re.compile(r"\bnow\s*\(\s*\)", re.IGNORECASE)
_PLACEHOLDER_RE = re.compile(r"\$\d+")
_WHITESPACE_RE = re.compile(r"\s+")
_MAX_SLOW_QUERIES_TO_FETCH = 200
_QUERY_FINGERPRINT_MAX_LEN = 80
_ALIAS_STOP_WORDS = {
    "where",
    "join",
    "on",
    "group",
    "order",
    "limit",
    "offset",
    "having",
    "union",
    "intersect",
    "except",
}
logger = logging.getLogger(__name__)


def _truncate(text: str, max_len: int = _MAX_QUERY_LEN) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _one_line(text: str) -> str:
    return " ".join(text.split())


def _is_potential_ddl(sql: str) -> bool:
    return bool(_DDL_PREFIX_RE.match(sql))


def _normalize_for_matching(sql: str) -> str:
    normalized = sql.strip().rstrip(";")
    normalized = _STRING_LITERAL_RE.sub("?", normalized)
    normalized = _NUMERIC_LITERAL_RE.sub("?", normalized)
    normalized = _NOW_LITERAL_RE.sub("?", normalized)
    normalized = _PLACEHOLDER_RE.sub("?", normalized)
    normalized = _WHITESPACE_RE.sub(" ", normalized)
    return normalized.lower()


def _table_tokens(table: str) -> set[str]:
    token = table.strip().strip('"').lower()
    if "." in token:
        return {token, token.rsplit(".", 1)[-1]}
    return {token}


def _extract_aliases_for_table(sql: str, table: str) -> set[str]:
    aliases: set[str] = set()
    target_tokens = _table_tokens(table)
    for match in _FROM_OR_JOIN_ALIAS_RE.finditer(sql):
        raw_table = match.group(1).strip().strip('"').lower()
        alias = (match.group(2) or "").strip().strip('"').lower()
        if raw_table in target_tokens:
            aliases.add(raw_table.rsplit(".", 1)[-1])
            if alias and alias not in _ALIAS_STOP_WORDS:
                aliases.add(alias)
    return aliases


def _predicate_chunks(sql: str) -> list[str]:
    chunks = [match.group(1) for match in _WHERE_RE.finditer(sql)]
    chunks.extend(match.group(1) for match in _JOIN_ON_RE.finditer(sql))
    return chunks


def _query_uses_columns_in_predicates(sql: str, table: str, columns: list[str]) -> bool:
    if not columns:
        return False
    aliases = _extract_aliases_for_table(sql, table)
    if not aliases:
        return False
    predicates = _predicate_chunks(sql)
    if not predicates:
        return False
    allow_bare_column = len(aliases) == 1
    for column in columns:
        qualified_patterns = [
            re.compile(rf"\b{re.escape(alias)}\s*\.\s*{re.escape(column.lower())}\b")
            for alias in aliases
        ]
        bare_pattern = re.compile(rf"(?<!\.)\b{re.escape(column.lower())}\b")
        column_match = False
        for predicate in predicates:
            pred = predicate.lower()
            qualified = any(pattern.search(pred) for pattern in qualified_patterns)
            bare = allow_bare_column and bare_pattern.search(pred)
            if qualified or bare:
                column_match = True
                break
        if not column_match:
            return False
    return True


def _query_fingerprint(query: str) -> str:
    one_line = _one_line(query)
    if len(one_line) <= _QUERY_FINGERPRINT_MAX_LEN:
        return one_line
    return one_line[: _QUERY_FINGERPRINT_MAX_LEN - 3] + "..."


def _enrich_recommendations_with_workload_benefits(
    recommendations: list[IndexRecommendation],
    slow_queries: list[SlowQuery],
    *,
    exclude_sql: str,
) -> None:
    if not recommendations or not slow_queries:
        return
    normalized_input_sql = _normalize_for_matching(exclude_sql)
    for recommendation in recommendations:
        recommendation.also_benefits = []
        recommendation.also_benefits_calls_per_day = 0
        for slow_query in slow_queries:
            if _normalize_for_matching(slow_query.query_text) == normalized_input_sql:
                continue
            if not _query_uses_columns_in_predicates(
                slow_query.query_text, recommendation.table, recommendation.columns
            ):
                continue
            recommendation.also_benefits_calls_per_day += slow_query.calls
            recommendation.also_benefits.append(
                f"{_query_fingerprint(slow_query.query_text)} "
                f"(called {slow_query.calls:,}/day)"
            )


def _overall_severity(issues: list[Issue]) -> str:
    from pgreviewer.core.models import Severity

    if any(i.severity == Severity.CRITICAL for i in issues):
        return "CRITICAL"
    if any(i.severity == Severity.WARNING for i in issues):
        return "WARNING"
    return "PASS"


def _print_rich_report(
    sql: str,
    result: AnalysisResult,
    *,
    verbose: bool = False,
    report_console: Console = console,
) -> None:
    issues = result.issues
    sev = _overall_severity(issues)
    badge = _SEVERITY_BADGE.get(sev, f"🟢 {sev}") if sev != "PASS" else "🟢 PASS"
    style = _SEVERITY_STYLE.get(sev, "green")
    n_critical = sum(1 for i in issues if i.severity.value == "CRITICAL")
    n_warning = sum(1 for i in issues if i.severity.value == "WARNING")
    total = len(issues)

    header = (
        f"[bold]Query[/bold]\n{_truncate(sql)}\n\n"
        f"[bold]Severity[/bold]: [{style}]{badge}[/{style}]\n"
        f"[bold]Issues found[/bold]: {total}"
    )
    report_console.print(Panel(header, title="pgReviewer Analysis", border_style=style))
    report_console.print()
    from pgreviewer.reporting.cli_report import generate_cli_report

    report_console.print(generate_cli_report(result))
    report_console.print()

    w_plural = "s" if n_warning != 1 else ""
    i_plural = "s" if total != 1 else ""
    report_console.print(
        f"[bold]{total} issue{i_plural} found "
        f"({n_critical} critical, {n_warning} warning{w_plural})[/bold]"
    )
    report_console.print()

    if verbose:
        report_console.rule("[bold]Issue Details[/bold]")
        explain_payload = None
        if result.raw_explain is not None:
            explain_payload = json.dumps(result.raw_explain, indent=2, default=str)
        if issues:
            for idx, issue in enumerate(issues, start=1):
                issue_style = _SEVERITY_STYLE.get(issue.severity.value, "white")
                lines = [
                    f"[bold]Detector:[/bold] {issue.detector_name}",
                    f"[bold]Description:[/bold] {issue.description}",
                    f"[bold]Suggested fix:[/bold] {issue.suggested_action}",
                ]
                if issue.context:
                    lines.append("[bold]Issue context:[/bold]")
                    lines.append(json.dumps(issue.context, indent=2, default=str))
                if explain_payload is not None:
                    lines.extend(["", "[bold]EXPLAIN JSON:[/bold]", explain_payload])
                report_console.print(
                    Panel(
                        "\n".join(lines),
                        title=f"Issue {idx}: {issue.severity.value}",
                        border_style=issue_style,
                    )
                )
        elif explain_payload is not None:
            report_console.print(
                Panel(
                    Syntax(explain_payload, "json"),
                    title="EXPLAIN JSON (verbose)",
                    border_style="cyan",
                )
            )
            report_console.print()

        if result.llm_interpretation is not None:
            report_console.print()
            report_console.print(
                Panel(
                    Syntax(
                        json.dumps(result.llm_interpretation, indent=2, default=str),
                        "json",
                    ),
                    title="LLM Interpretation (verbose)",
                    border_style="cyan",
                )
            )
            report_console.print()


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


def _print_recommendations(
    recs: list[IndexRecommendation], report_console: Console = console
) -> None:
    if not recs:
        return

    high_and_moderate = [
        rec for rec in recs if rec.confidence >= _RECOMMENDATION_MODERATE_CONFIDENCE
    ]
    low_confidence = [
        rec for rec in recs if rec.confidence < _RECOMMENDATION_MODERATE_CONFIDENCE
    ]

    if high_and_moderate:
        report_console.rule("[bold]Recommended Indexes[/bold]")
    for rec in high_and_moderate:
        if rec.validated:
            title = "💡 Suggested index (HypoPG validated ✓)"
            style = "green"
        else:
            title = "⚠️  Suggested index (Not validated)"
            style = "yellow"

        panel_items: list[Any] = [
            Syntax(rec.create_statement, "sql", word_wrap=True),
            (
                f"Cost: {rec.cost_before:.2f} → {rec.cost_after:.2f}  "
                f"(improvement: {rec.improvement_pct * 100:.1f}%)"
            ),
        ]
        if rec.rationale:
            panel_items.append(f"[dim]Rationale: {rec.rationale}[/dim]")
        if (
            _RECOMMENDATION_MODERATE_CONFIDENCE
            <= rec.confidence
            < _RECOMMENDATION_NORMAL_CONFIDENCE
        ):
            panel_items.append(
                "[yellow]⚠️  moderate confidence — verify before applying[/yellow]"
            )
        if rec.also_benefits:
            query_count = len(rec.also_benefits)
            query_label = "query" if query_count == 1 else "queries"
            panel_items.append(
                "This index would also improve "
                f"{query_count} other {query_label} in pg_stat_statements "
                f"(combined {rec.also_benefits_calls_per_day:,} calls/day)"
            )
            panel_items.extend(f"  - {query}" for query in rec.also_benefits)
        panel_items.extend([f"[yellow]⚠ Note: {note}[/yellow]" for note in rec.notes])
        report_console.print(
            Panel(
                Group(*panel_items),
                title=title,
                border_style=style,
            )
        )
        report_console.print()

    if low_confidence:
        report_console.rule("[bold]Possible issues (low confidence)[/bold]")
        for rec in low_confidence:
            panel_items = [Syntax(rec.create_statement, "sql", word_wrap=True)]
            panel_items.append("[yellow]🔍 manual review recommended[/yellow]")
            if rec.rationale:
                panel_items.append(f"[dim]Rationale: {rec.rationale}[/dim]")
            panel_items.extend(
                [f"[yellow]⚠ Note: {note}[/yellow]" for note in rec.notes]
            )
            report_console.print(
                Panel(
                    Group(*panel_items),
                    title="Low-confidence recommendation",
                    border_style="yellow",
                )
            )
            report_console.print()

    if len(recs) > 3:
        report_console.print(
            "[yellow]⚠ Adding more indexes may have diminishing "
            "write-performance returns. Profile before applying all.[/yellow]"
        )
        report_console.print()


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
    verbose: bool = False,
    no_color: bool = False,
) -> None:
    """Core logic for the ``pgr check`` command."""
    from pgreviewer.config import ConfigError, load_runtime_config

    report_console = Console(no_color=no_color)
    report_err_console = Console(stderr=True, no_color=no_color)
    try:
        runtime_config = load_runtime_config()
    except ConfigError as exc:
        report_err_console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(code=1) from None

    # --- Resolve the SQL string -------------------------------------------
    if query_file is not None:
        sql = query_file.read_text().strip()
    elif query is not None:
        sql = query.strip()
    else:
        report_err_console.print(
            "[red]Error:[/red] Provide a SQL query or --query-file."
        )
        raise typer.Exit(code=1)

    if not sql:
        report_err_console.print("[red]Error:[/red] SQL query must not be empty.")
        raise typer.Exit(code=1)

    # --- Run the async analysis pipeline ----------------------------------
    try:
        result = asyncio.run(_analyse_query_with_config(sql, runtime_config))
    except Exception as exc:
        report_err_console.print(f"[red]Error:[/red] {exc}")
        if "--debug" in sys.argv:
            import traceback

            traceback.print_exc()
        raise typer.Exit(code=1) from None

    if json_output:
        _print_json_report(sql, result)
    else:
        _print_rich_report(sql, result, verbose=verbose, report_console=report_console)
        if _is_potential_ddl(sql):
            report_console.print(
                Panel(
                    "DDL detected. Review lock scope, migration duration, "
                    "and rollback plan before applying in production.",
                    title="Migration safety",
                    border_style="yellow",
                )
            )
            report_console.print()
        if result.llm_degraded:
            msg = result.degradation_reason or "LLM analysis unavailable"
            report_console.print(
                Panel(
                    f"⚠️  {msg} — showing algorithmic analysis only",
                    title="Degradation notice",
                    border_style="yellow",
                )
            )


async def _analyse_query(sql: str) -> AnalysisResult:
    """Internal analysis pipeline."""
    from pgreviewer.config import load_runtime_config

    runtime_config = load_runtime_config()
    return await _analyse_query_with_config(sql, runtime_config)


async def _analyse_query_with_config(
    sql: str, runtime_config: RuntimeConfig
) -> AnalysisResult:
    from pgreviewer.analysis.complexity_router import should_use_llm
    from pgreviewer.analysis.index_generator import generate_create_index
    from pgreviewer.analysis.index_suggester import IndexCandidate
    from pgreviewer.analysis.issue_detectors import run_all_detectors
    from pgreviewer.analysis.plan_parser import extract_tables, parse_explain
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

    runtime_settings = runtime_config.runtime_settings
    result = AnalysisResult()
    backend = get_backend(runtime_settings)

    try:
        # 1. Broad analysis (Read-only)
        raw_plan = await backend.get_explain_plan(sql)
        result.raw_explain = raw_plan
        plan = parse_explain(raw_plan)
        tables = extract_tables(plan)
        if tables:
            table_infos = await asyncio.gather(
                *(backend.get_schema_info(table_name) for table_name in tables)
            )
            schema = SchemaInfo(tables=dict(zip(tables, table_infos, strict=False)))
        else:
            schema = SchemaInfo()
        issues = run_all_detectors(
            plan,
            schema,
            disabled_detectors=runtime_settings.DISABLED_DETECTORS,
            project_config=runtime_config.project,
            runtime_settings=runtime_settings,
        )

        recommendations = await backend.recommend_indexes([sql])
        recommendations.sort(key=lambda r: r.improvement_pct, reverse=True)
        _detect_redundant_recommendations(recommendations)

        result.issues = issues
        result.recommendations = recommendations

        # 6. LLM interpretation + validation for index suggestions
        if runtime_settings.LLM_API_KEY:
            use_llm, route_reason = should_use_llm(plan, issues)
            store = DebugStore(runtime_settings.DEBUG_STORE_PATH)
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
                    result.llm_interpretation = interpretation.model_dump(mode="json")

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
                                improvement_pct
                                >= runtime_settings.HYPOPG_MIN_IMPROVEMENT
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

        try:
            slow_queries = await backend.get_slow_queries(
                limit=_MAX_SLOW_QUERIES_TO_FETCH
            )
        except Exception as exc:
            logger.warning(
                "Skipping recommendation workload correlation: "
                "unable to fetch slow queries: %s",
                exc,
            )
        else:
            _enrich_recommendations_with_workload_benefits(
                result.recommendations, slow_queries, exclude_sql=sql
            )

        return result
    finally:
        if runtime_settings.BACKEND.lower() in {"local", "hybrid"}:
            from pgreviewer.db.pool import close_pool

            await close_pool()
