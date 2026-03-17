"""Implementation of the ``pgr diff`` command."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path
from typing import TYPE_CHECKING

import typer
from rich.console import Console
from rich.table import Table

from pgreviewer.analysis.workload_correlator import correlate as correlate_workload

if TYPE_CHECKING:
    from pgreviewer.core.degradation import AnalysisResult
    from pgreviewer.core.models import (
        ExtractedQuery,
        IndexRecommendation,
        Issue,
        SchemaInfo,
        SlowQuery,
    )

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

_DEFAULT_TRIGGER_PATHS = (
    "**.sql",
    "**.py",
    "**/migrations/**",
    "**/models.py",
    "**/models/**/*.py",
)


@dataclass
class WorkloadMatch:
    issue: Issue
    slow_query: SlowQuery


def _truncate(text: str, max_len: int = 80) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _overall_severity(issues: list[Issue]) -> str:
    from pgreviewer.core.models import Severity

    if any(i.severity == Severity.CRITICAL for i in issues):
        return "CRITICAL"
    if any(i.severity == Severity.WARNING for i in issues):
        return "WARNING"
    if issues:
        return "INFO"
    return "PASS"


def _has_critical_findings(
    results: list[dict],
    model_diff_results: list[dict],
    cross_cutting_findings: list,
) -> bool:
    return (
        any(
            issue.severity.value == "CRITICAL"
            for item in results
            for issue in item["analysis_result"].issues
        )
        or any(
            issue.severity.value == "CRITICAL"
            for entry in model_diff_results
            for issue in entry.get("model_issues", [])
        )
        or any(
            finding.issue.severity.value == "CRITICAL"
            for finding in cross_cutting_findings
        )
    )


def _count_issues_by_severity(
    results: list[dict],
    model_diff_results: list[dict],
    cross_cutting_findings: list,
    code_pattern_issues: list[Issue] | None = None,
) -> dict[str, int]:
    counts = {"CRITICAL": 0, "WARNING": 0, "INFO": 0}
    for item in results:
        for issue in item["analysis_result"].issues:
            if issue.severity.value in counts:
                counts[issue.severity.value] += 1
    for entry in model_diff_results:
        for issue in entry.get("model_issues", []):
            if issue.severity.value in counts:
                counts[issue.severity.value] += 1
    for finding in cross_cutting_findings:
        if finding.issue.severity.value in counts:
            counts[finding.issue.severity.value] += 1
    for issue in code_pattern_issues or []:
        if issue.severity.value in counts:
            counts[issue.severity.value] += 1
    return counts


def _threshold_violated(severity_threshold: str, counts: dict[str, int]) -> bool:
    if severity_threshold == "none":
        return False
    if severity_threshold == "critical":
        return counts["CRITICAL"] > 0
    if severity_threshold == "warning":
        return counts["CRITICAL"] > 0 or counts["WARNING"] > 0
    if severity_threshold == "info":
        return counts["CRITICAL"] > 0 or counts["WARNING"] > 0 or counts["INFO"] > 0
    raise ValueError(f"Unsupported severity threshold: {severity_threshold}")


def _is_llm_degraded(results: list[dict]) -> bool:
    return any(item["analysis_result"].llm_degraded for item in results)


def _get_file_at_ref(ref: str, file_path: str) -> str | None:
    """Return the UTF-8 content of *file_path* at the given git *ref*.

    Runs ``git show <ref>:<file_path>`` and returns the decoded output, or
    ``None`` if the file does not exist at that ref or git is unavailable.

    Parameters
    ----------
    ref:
        A git ref (branch name, tag, commit SHA, or ``HEAD``).
    file_path:
        Path to the file relative to the repository root.

    Returns
    -------
    str | None
        File content, or ``None`` when the file cannot be retrieved.
    """
    import subprocess

    try:
        result = subprocess.run(
            ["git", "show", f"{ref}:{file_path}"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return result.stdout
        return None
    except FileNotFoundError:
        return None


def _get_git_diff(git_ref: str | None = None, staged: bool = False) -> str:
    """Run *git diff* and return the output as a string.

    Args:
        git_ref: A git ref (branch, tag, commit SHA, or expression like HEAD~1).
            When provided, runs ``git diff <git_ref>``.
        staged: When *True* runs ``git diff --staged``.

    Returns:
        The diff output as a UTF-8 string.

    Raises:
        ValueError: When git is not installed, the working directory is not a
            git repository, or the supplied ref is invalid.
    """
    import subprocess

    cmd: list[str] = (
        ["git", "diff", "--staged"] if staged else ["git", "diff", git_ref]  # type: ignore[list-item]
    )

    if not staged and git_ref is None:
        raise ValueError("Either git_ref or staged=True must be provided")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        raise ValueError("git is not installed or not found in PATH") from None

    if result.returncode != 0:
        stderr = result.stderr.strip()
        lower = stderr.lower()
        if "not a git repository" in lower:
            raise ValueError("Not inside a git repository")
        if git_ref and (
            "unknown revision" in lower
            or "bad revision" in lower
            or "ambiguous argument" in lower
        ):
            raise ValueError(f"Invalid git ref '{git_ref}': {stderr}")
        raise ValueError(f"git diff failed: {stderr}")

    return result.stdout


def _path_matches_trigger(path: str, pattern: str) -> bool:
    normalised = path.replace("\\", "/")
    if fnmatch(normalised, pattern):
        return True
    if pattern.startswith("**/"):
        return fnmatch(normalised, pattern[3:])
    return False


def _is_trigger_candidate(
    path: str, trigger_patterns: list[str] | tuple[str, ...]
) -> bool:
    return any(_path_matches_trigger(path, pattern) for pattern in trigger_patterns)


_MS_PER_MINUTE = 60_000
_CRITICAL_AVG_TIME_MS_THRESHOLD = 1_000
_HIGH_VOLUME_CALLS_THRESHOLD = 1_000
logger = logging.getLogger(__name__)


def _find_workload_matches(
    results: list[dict], slow_queries: list[SlowQuery]
) -> list[WorkloadMatch]:
    """Return issue-level matches for queries found in production slow workload."""
    if not results or not slow_queries:
        return []

    extracted_queries = [item["query_obj"] for item in results]
    workload_matches = correlate_workload(extracted_queries, slow_queries)
    slow_query_by_extracted_query = {
        id(match.extracted_query): match.slow_query for match in workload_matches
    }
    matches: list[WorkloadMatch] = []
    for item in results:
        query_obj: ExtractedQuery = item["query_obj"]
        matched_slow_query = slow_query_by_extracted_query.get(id(query_obj))
        if matched_slow_query is None:
            continue
        for issue in item.get("issues", []):
            matches.append(WorkloadMatch(issue=issue, slow_query=matched_slow_query))
    return matches


def _apply_workload_correlation(
    results: list[dict], slow_queries: list[SlowQuery]
) -> None:
    """Attach workload stats and escalate severity for matched query issues."""
    from pgreviewer.core.models import Severity

    for match in _find_workload_matches(results, slow_queries):
        issue = match.issue
        slow_query = match.slow_query

        context = issue.context or {}
        context["workload_stats"] = {
            "calls_per_day": slow_query.calls,
            "avg_time_ms": slow_query.mean_exec_time_ms,
            "total_time_min_per_day": slow_query.total_exec_time_ms / _MS_PER_MINUTE,
        }
        issue.context = context

        if slow_query.mean_exec_time_ms > _CRITICAL_AVG_TIME_MS_THRESHOLD:
            issue.severity = Severity.CRITICAL
        elif (
            slow_query.calls > _HIGH_VOLUME_CALLS_THRESHOLD
            and issue.severity == Severity.INFO
        ):
            issue.severity = Severity.WARNING


def _model_diffs_have_removed_indexes(model_diff_results: list[dict]) -> bool:
    return any(
        diff.removed_indexes for entry in model_diff_results for diff in entry["diffs"]
    )


def _apply_removed_index_workload_correlation(
    model_diff_results: list[dict], slow_queries: list[SlowQuery]
) -> None:
    from pgreviewer.analysis.migration_detectors.drop_index_workload import (
        detect_removed_index_workload_issues,
    )

    if not model_diff_results or not slow_queries:
        return

    for entry in model_diff_results:
        model_issues = entry.setdefault("model_issues", [])
        for diff in entry["diffs"]:
            model_issues.extend(
                detect_removed_index_workload_issues(diff, slow_queries)
            )


async def _fetch_slow_queries(limit: int = 200) -> list[SlowQuery]:
    from pgreviewer.config import settings
    from pgreviewer.core.backend import get_backend

    return await get_backend(settings).get_slow_queries(limit=limit)


def run_diff(
    diff_file: Path | None,
    json_output: bool,
    only_critical: bool,
    git_ref: str | None = None,
    staged: bool = False,
    ci: bool = False,
    severity_threshold: str = "critical",
) -> None:
    # Validate that exactly one input source is provided (before any heavy imports)
    sources = sum([diff_file is not None, git_ref is not None, staged])
    if sources == 0:
        err_console.print(
            "[red]Error:[/red] Provide a diff file, --git-ref, or --staged."
        )
        raise typer.Exit(code=1)
    if sources > 1:
        err_console.print(
            "[red]Error:[/red] Use only one of: diff file, --git-ref, or --staged."
        )
        raise typer.Exit(code=1)

    if diff_file is not None:
        try:
            diff_content = diff_file.read_text(encoding="utf-8")
        except Exception as e:
            err_console.print(f"[red]Error reading diff file:[/red] {e}")
            raise typer.Exit(code=1) from e
    else:
        try:
            diff_content = _get_git_diff(git_ref=git_ref, staged=staged)
        except ValueError as e:
            err_console.print(f"[red]Error:[/red] {e}")
            raise typer.Exit(code=1) from e

    from pgreviewer.parsing.diff_parser import parse_diff

    changed_files = parse_diff(diff_content)
    if not changed_files:
        if not json_output:
            console.print("No SQL changes detected.")
        else:
            sys.stdout.write(json.dumps({}) + "\n")
        return

    from pgreviewer.config import settings

    trigger_patterns = settings.TRIGGER_PATHS or list(_DEFAULT_TRIGGER_PATHS)
    changed_files = [
        cf for cf in changed_files if _is_trigger_candidate(cf.path, trigger_patterns)
    ]
    if not changed_files:
        if not json_output:
            console.print("No SQL changes detected.")
        else:
            sys.stdout.write(
                json.dumps({"skipped": [], "results": []}, indent=2) + "\n"
            )
        return

    from pgreviewer.analysis.code_pattern_detectors import ParsedFile
    from pgreviewer.parsing.extraction_router import route_extraction
    from pgreviewer.parsing.file_classifier import FileType, classify_file
    from pgreviewer.parsing.treesitter import TSParser

    # Determine the "before" git ref for model diffing.
    # git_ref → compare against that ref; --staged → compare against HEAD.
    # diff_file → no git context, skip model diffing.
    before_ref: str | None = git_ref if git_ref else ("HEAD" if staged else None)

    extracted_queries: list[ExtractedQuery] = []
    skipped_files: list[dict[str, str]] = []
    parsed_files: list[ParsedFile] = []
    # list of {"file": str, "diffs": list[ModelDiff]}
    model_diff_results: list[dict] = []
    has_python_candidates = any(cf.path.endswith(".py") for cf in changed_files)
    ts_parser = TSParser(default_language="python") if has_python_candidates else None

    for cf in changed_files:
        path_str = cf.path
        local_path = Path(path_str)

        if not local_path.is_file():
            skipped_files.append({"file": path_str, "reason": "File not found on disk"})
            continue

        try:
            full_text = local_path.read_text(encoding="utf-8")
        except Exception as e:
            skipped_files.append({"file": path_str, "reason": f"Read error: {e}"})
            continue

        file_type = classify_file(path_str, full_text)

        if file_type == FileType.IGNORE:
            skipped_files.append({"file": path_str, "reason": "Ignored by classifier"})
            continue

        try:
            queries = route_extraction(cf, file_type)

            if not queries:
                skipped_files.append({"file": path_str, "reason": "No SQL found"})
            else:
                extracted_queries.extend(queries)
        except Exception as e:
            skipped_files.append(
                {"file": path_str, "reason": f"Extraction failed: {e}"}
            )

        # --- Model diff (MIGRATION_PYTHON and PYTHON_WITH_SQL only) -------
        if file_type in (FileType.MIGRATION_PYTHON, FileType.PYTHON_WITH_SQL):
            _collect_model_diffs(path_str, full_text, before_ref, model_diff_results)

        if local_path.suffix == ".py":
            if ts_parser is None:
                raise RuntimeError(
                    "tree-sitter parser not initialized for python files"
                )
            parsed_files.append(
                ParsedFile(
                    path=path_str,
                    tree=ts_parser.parse_file(full_text, language="python"),
                    language="python",
                    content=full_text,
                )
            )

    has_any_analysis_inputs = (
        bool(extracted_queries) or bool(model_diff_results) or bool(parsed_files)
    )
    if not has_any_analysis_inputs:
        if not json_output:
            console.print("No SQL changes detected.")
        else:
            sys.stdout.write(
                json.dumps({"skipped": skipped_files, "results": []}, indent=2) + "\n"
            )
        return

    # Run Analysis (only when there are SQL queries to analyze)
    from pgreviewer.analysis.cross_correlator import correlate_findings

    results: list[dict] = []
    cross_cutting_findings = []
    code_pattern_issues: list[Issue] = []
    if extracted_queries:
        try:
            results = asyncio.run(
                _analyze_all_queries(extracted_queries, only_critical)
            )
            cross_cutting_findings = correlate_findings(results)
            if only_critical:
                for item in results:
                    item["issues"] = [
                        i for i in item["issues"] if i.severity.value == "CRITICAL"
                    ]
        except Exception as exc:
            err_console.print(f"[red]Analysis Error:[/red] {exc}")
            raise typer.Exit(code=1) from None
    if parsed_files:
        from pgreviewer.analysis.code_pattern_detectors import (
            run_code_pattern_detectors,
        )
        from pgreviewer.analysis.query_catalog import build_catalog
        from pgreviewer.config import settings

        query_catalog = build_catalog(Path.cwd())
        code_pattern_issues = run_code_pattern_detectors(
            parsed_files,
            query_catalog,
            disabled_detectors=settings.DISABLED_DETECTORS,
        )
    if _model_diffs_have_removed_indexes(model_diff_results):
        try:
            slow_queries = asyncio.run(_fetch_slow_queries(limit=200))
        except Exception as exc:
            logger.debug(
                "Skipping dropped-index workload correlation for model diffs: %s",
                exc,
            )
            slow_queries = []
        _apply_removed_index_workload_correlation(model_diff_results, slow_queries)

    if json_output:
        _print_json_diff_report(
            results,
            skipped_files,
            model_diff_results,
            cross_cutting_findings,
            code_pattern_issues,
        )
    else:
        _print_rich_diff_report(
            results,
            skipped_files,
            model_diff_results,
            cross_cutting_findings,
            code_pattern_issues,
        )

    if ci:
        threshold = severity_threshold.lower()
        counts = _count_issues_by_severity(
            results,
            model_diff_results,
            cross_cutting_findings,
            code_pattern_issues,
        )
        failed = _threshold_violated(threshold, counts)
        result_label = "FAIL" if failed else "PASS"
        console.print(
            "Severity threshold: "
            f"{threshold}. Found: {counts['CRITICAL']} critical, "
            f"{counts['WARNING']} warning, "
            f"{counts['INFO']} info. Result: {result_label}"
        )
        if _is_llm_degraded(results):
            conclusion = "neutral"
        elif failed:
            conclusion = "failure"
        else:
            conclusion = "success"
        console.print(f"Check run conclusion: {conclusion}")
        if failed:
            raise typer.Exit(code=1)
        return

    if _has_critical_findings(results, model_diff_results, cross_cutting_findings):
        raise typer.Exit(code=2)


async def _analyze_all_queries(
    extracted_queries: list[ExtractedQuery], only_critical: bool
) -> list[dict]:
    from pgreviewer.analysis.migration_detectors import (
        parse_ddl_statement,
        run_migration_detectors,
    )
    from pgreviewer.analysis.migration_detectors.drop_index_workload import (
        detect_drop_index_workload_issues,
    )
    from pgreviewer.cli.commands.check import _analyse_query, _is_potential_ddl
    from pgreviewer.core.degradation import AnalysisResult
    from pgreviewer.core.models import ParsedMigration, SchemaInfo

    results = []
    for q in extracted_queries:
        parsed_migration = ParsedMigration(
            statements=[parse_ddl_statement(q.sql, q.line_number)],
            source_file=q.source_file,
            extracted_queries=extracted_queries,
        )
        if _is_potential_ddl(q.sql):
            # DDL cannot be EXPLAINed — run migration detectors only.
            migration_issues = await asyncio.to_thread(
                run_migration_detectors,
                parsed_migration,
                SchemaInfo(),
            )
            result = AnalysisResult(issues=migration_issues)
        else:
            gathered_results = await asyncio.gather(
                _analyse_query(q.sql),
                asyncio.to_thread(
                    run_migration_detectors,
                    parsed_migration,
                    SchemaInfo(),
                ),
            )
            result, migration_issues = gathered_results
            result.issues.extend(migration_issues)
        results.append(
            {
                "query_obj": q,
                "analysis_result": result,
                "issues": result.issues,
                "recs": result.recommendations,
            }
        )
    try:
        slow_queries = await _fetch_slow_queries(limit=200)
    except Exception as exc:
        logger.debug(
            "Skipping workload correlation: unable to fetch slow queries: %s", exc
        )
        slow_queries = []
    for item in results:
        query_obj: ExtractedQuery = item["query_obj"]
        parsed_migration = ParsedMigration(
            statements=[parse_ddl_statement(query_obj.sql, query_obj.line_number)],
            source_file=query_obj.source_file,
            extracted_queries=extracted_queries,
        )
        drop_index_workload_issues = detect_drop_index_workload_issues(
            parsed_migration, slow_queries
        )
        item["analysis_result"].issues.extend(drop_index_workload_issues)
        item["issues"] = item["analysis_result"].issues
    _apply_workload_correlation(results, slow_queries)
    return results


def _collect_model_diffs(
    path_str: str,
    full_text: str,
    before_ref: str | None,
    model_diff_results: list[dict],
) -> None:
    """Parse *path_str* as a SQLAlchemy model file and append diffs to
    *model_diff_results*.

    Compares the current file content against the version at *before_ref* (via
    ``git show``).  If *before_ref* is ``None`` or the file cannot be retrieved,
    all models in the current version are treated as entirely new.

    Parameters
    ----------
    path_str:
        Repository-relative (or absolute) path of the Python file.
    full_text:
        Current (after) content of the file.
    before_ref:
        Git ref for the base state, or ``None`` to skip git retrieval.
    model_diff_results:
        Output list to append ``{"file": str, "diffs": list[ModelDiff]}`` to.
    """
    import logging

    from pgreviewer.parsing.model_differ import ModelDiff, diff_models
    from pgreviewer.parsing.sqlalchemy_analyzer import (
        ModelDefinition,
        analyze_model_source,
    )

    logger = logging.getLogger(__name__)

    try:
        after_models = analyze_model_source(full_text, file_path=path_str)
    except Exception as exc:
        logger.debug("Model parse failed for %s (after): %s", path_str, exc)
        return

    if not after_models:
        return

    # Retrieve the before content via git show.
    before_content: str | None = None
    if before_ref is not None:
        before_content = _get_file_at_ref(before_ref, path_str)

    try:
        before_models: list[ModelDefinition] = (
            analyze_model_source(before_content, file_path=path_str)
            if before_content
            else []
        )
    except Exception as exc:
        logger.debug("Model parse failed for %s (before): %s", path_str, exc)
        before_models = []

    before_by_name = {m.class_name: m for m in before_models}
    diffs: list[ModelDiff] = []

    for after_model in after_models:
        before_model = before_by_name.get(after_model.class_name)
        if before_model is None:
            # New model class – treat everything as added.
            before_model = ModelDefinition(
                class_name=after_model.class_name,
                table_name=after_model.table_name,
            )
        diff = diff_models(before_model, after_model)
        if diff.has_changes:
            diffs.append(diff)

    if diffs:
        from pgreviewer.analysis.model_issue_detectors import run_model_issue_detectors

        model_issues = []
        for d in diffs:
            model_issues.extend(run_model_issue_detectors(d))
        model_diff_results.append(
            {"file": path_str, "diffs": diffs, "model_issues": model_issues}
        )


def _print_rich_diff_report(
    results: list[dict],
    skipped_files: list[dict],
    model_diff_results: list[dict] | None = None,
    cross_cutting_findings: list | None = None,
    code_pattern_issues: list[Issue] | None = None,
    schema: SchemaInfo | None = None,
) -> None:
    from pgreviewer.cli.commands.check import _print_recommendations
    from pgreviewer.core.models import SchemaInfo

    console.rule("[bold]pgReviewer Diff Analysis[/bold]")
    schema = schema or SchemaInfo()

    if skipped_files:
        console.print("[bold]Skipped Files:[/bold]")
        for s in skipped_files:
            console.print(f"  - [dim]{s['file']}[/dim]: {s['reason']}")
        console.print()

    # Group by file
    grouped = defaultdict(list)
    for res in results:
        grouped[res["query_obj"].source_file].append(res)

    for file_path, items in grouped.items():
        console.rule(f"[bold cyan]File: {file_path}[/bold cyan]")

        for item in items:
            q: ExtractedQuery = item["query_obj"]
            result: AnalysisResult = item["analysis_result"]
            issues: list[Issue] = result.issues
            recs: list[IndexRecommendation] = result.recommendations

            if result.llm_degraded:
                msg = result.degradation_reason or "LLM analysis unavailable"
                console.print(
                    f"  [yellow]⚠️  {msg} — showing algorithmic analysis only[/yellow]"
                )

            sev = _overall_severity(issues)
            badge = (
                _SEVERITY_BADGE.get(sev, f"🟢 {sev}") if sev != "PASS" else "🟢 PASS"
            )
            style = _SEVERITY_STYLE.get(sev, "green")

            console.print(f"[bold]Line {q.line_number}:[/bold] {_truncate(q.sql)}")
            console.print(f"[bold]Overall:[/bold] [{style}]{badge}[/{style}]")

            if issues:
                table = Table(show_header=True, header_style="bold cyan", expand=True)
                table.add_column("Severity", style="bold", width=10)
                table.add_column("Detector", width=28)
                table.add_column("Description")
                table.add_column("Suggested Action")

                for issue in issues:
                    row_style = _SEVERITY_STYLE.get(issue.severity.value, "")
                    sev_label = _SEVERITY_BADGE.get(
                        issue.severity.value, issue.severity.value
                    )
                    table.add_row(
                        f"[{row_style}]{sev_label}[/{row_style}]",
                        issue.detector_name,
                        issue.description,
                        issue.suggested_action,
                    )

                console.print(table)

            _print_recommendations(recs)
            console.print()

    # Model diff section
    if model_diff_results:
        console.rule("[bold]Model Changes[/bold]")
        for entry in model_diff_results:
            console.print(f"[bold cyan]File: {entry['file']}[/bold cyan]")
            for diff in entry["diffs"]:
                console.print(
                    f"  [bold]{diff.class_name}[/bold] ([dim]{diff.table_name}[/dim]):"
                )
                for col in diff.added_columns:
                    console.print(
                        f"    [green]+ column:[/green] {col.name} ({col.col_type})"
                    )
                for col in diff.removed_columns:
                    console.print(
                        f"    [red]- column:[/red] {col.name} ({col.col_type})"
                    )
                for idx in diff.added_indexes:
                    cols = ", ".join(idx.columns)
                    console.print(f"    [green]+ index:[/green] {idx.name} ({cols})")
                for idx in diff.removed_indexes:
                    cols = ", ".join(idx.columns)
                    console.print(f"    [red]- index:[/red] {idx.name} ({cols})")
                for rel in diff.added_relationships:
                    console.print(
                        f"    [green]+ relationship:[/green]"
                        f" {rel.name} → {rel.target_model}"
                    )
                for rel in diff.removed_relationships:
                    console.print(
                        f"    [red]- relationship:[/red]"
                        f" {rel.name} → {rel.target_model}"
                    )

            model_issues: list[Issue] = entry.get("model_issues", [])
            if model_issues:
                console.print()
                console.print(f"  [bold]Model Issues ({len(model_issues)}):[/bold]")
                tbl = Table(show_header=True, header_style="bold cyan", expand=True)
                tbl.add_column("Severity", style="bold", width=10)
                tbl.add_column("Detector", width=28)
                tbl.add_column("Description")
                tbl.add_column("Suggested Action")
                for issue in model_issues:
                    row_style = _SEVERITY_STYLE.get(issue.severity.value, "")
                    sev_label = _SEVERITY_BADGE.get(
                        issue.severity.value, issue.severity.value
                    )
                    tbl.add_row(
                        f"[{row_style}]{sev_label}[/{row_style}]",
                        issue.detector_name,
                        issue.description,
                        issue.suggested_action,
                    )
                console.print(tbl)
            console.print()

    if cross_cutting_findings:
        console.rule("[bold]Cross-cutting Findings[/bold]")
        table = Table(show_header=True, header_style="bold cyan", expand=True)
        table.add_column("Severity", style="bold", width=10)
        table.add_column("Detector", width=40)
        table.add_column("Description")
        table.add_column("Sources")
        for finding in cross_cutting_findings:
            issue = finding.issue
            row_style = _SEVERITY_STYLE.get(issue.severity.value, "")
            sev_label = _SEVERITY_BADGE.get(issue.severity.value, issue.severity.value)
            table.add_row(
                f"[{row_style}]{sev_label}[/{row_style}]",
                issue.detector_name,
                issue.description,
                (
                    f"migration: {finding.migration_file}:{finding.migration_line}\n"
                    f"query: {finding.query_file}:{finding.query_line}"
                ),
            )
        console.print(table)

    if code_pattern_issues:
        console.rule("[bold]Code Pattern Issues[/bold]")
        table = Table(show_header=True, header_style="bold cyan", expand=True)
        table.add_column("Severity", style="bold", width=10)
        table.add_column("Detector", width=40)
        table.add_column("Description")
        table.add_column("Suggested Action")
        for issue in code_pattern_issues:
            description = issue.description
            if issue.detector_name == "query_in_loop":
                from pgreviewer.analysis.impact_estimator import estimate_loop_impact

                impact = estimate_loop_impact(issue, schema)
                description = f"{description}\nImpact: {impact.summary}"
            row_style = _SEVERITY_STYLE.get(issue.severity.value, "")
            sev_label = _SEVERITY_BADGE.get(issue.severity.value, issue.severity.value)
            table.add_row(
                f"[{row_style}]{sev_label}[/{row_style}]",
                issue.detector_name,
                description,
                issue.suggested_action,
            )
        console.print(table)


def _print_json_diff_report(
    results: list[dict],
    skipped_files: list[dict],
    model_diff_results: list[dict] | None = None,
    cross_cutting_findings: list | None = None,
    code_pattern_issues: list[Issue] | None = None,
    schema: SchemaInfo | None = None,
) -> None:
    from pgreviewer.analysis.impact_estimator import estimate_loop_impact
    from pgreviewer.core.models import SchemaInfo

    schema = schema or SchemaInfo()
    output_results = []
    for item in results:
        q: ExtractedQuery = item["query_obj"]
        result: AnalysisResult = item["analysis_result"]
        issues: list[Issue] = result.issues
        recs: list[IndexRecommendation] = result.recommendations

        output_results.append(
            {
                "source_file": q.source_file,
                "line_number": q.line_number,
                "extraction_method": q.extraction_method,
                "confidence": q.confidence,
                "query": q.sql,
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
        )

    # Serialize model diffs
    serialized_model_diffs = []
    for entry in model_diff_results or []:
        entry_model_issues: list[Issue] = entry.get("model_issues", [])
        serialized_model_diffs.append(
            {
                "file": entry["file"],
                "diffs": [
                    {
                        "class_name": d.class_name,
                        "table_name": d.table_name,
                        "added_columns": [
                            {"name": c.name, "col_type": c.col_type}
                            for c in d.added_columns
                        ],
                        "removed_columns": [
                            {"name": c.name, "col_type": c.col_type}
                            for c in d.removed_columns
                        ],
                        "added_indexes": [
                            {
                                "name": i.name,
                                "columns": i.columns,
                                "is_unique": i.is_unique,
                            }
                            for i in d.added_indexes
                        ],
                        "removed_indexes": [
                            {
                                "name": i.name,
                                "columns": i.columns,
                                "is_unique": i.is_unique,
                            }
                            for i in d.removed_indexes
                        ],
                        "added_relationships": [
                            {"name": r.name, "target_model": r.target_model}
                            for r in d.added_relationships
                        ],
                        "removed_relationships": [
                            {"name": r.name, "target_model": r.target_model}
                            for r in d.removed_relationships
                        ],
                    }
                    for d in entry["diffs"]
                ],
                "model_issues": [
                    {
                        "severity": i.severity.value,
                        "detector_name": i.detector_name,
                        "description": i.description,
                        "affected_table": i.affected_table,
                        "affected_columns": i.affected_columns,
                        "suggested_action": i.suggested_action,
                        "confidence": i.confidence,
                    }
                    for i in entry_model_issues
                ],
            }
        )

    output_payload = {
        "skipped": skipped_files,
        "results": output_results,
        "model_diffs": serialized_model_diffs,
        "cross_cutting_findings": [
            {
                "severity": finding.issue.severity.value,
                "detector_name": finding.issue.detector_name,
                "description": finding.issue.description,
                "affected_table": finding.issue.affected_table,
                "affected_columns": finding.issue.affected_columns,
                "suggested_action": finding.issue.suggested_action,
                "migration_source": {
                    "file": finding.migration_file,
                    "line_number": finding.migration_line,
                },
                "query_source": {
                    "file": finding.query_file,
                    "line_number": finding.query_line,
                },
            }
            for finding in cross_cutting_findings or []
        ],
        "code_pattern_issues": [
            {
                "severity": i.severity.value,
                "detector_name": i.detector_name,
                "description": i.description,
                "affected_table": i.affected_table,
                "affected_columns": i.affected_columns,
                "suggested_action": i.suggested_action,
                "confidence": i.confidence,
                "impact_estimate": (
                    None
                    if i.detector_name != "query_in_loop"
                    else estimate_loop_impact(i, schema).to_dict()
                ),
            }
            for i in code_pattern_issues or []
        ],
    }
    sys.stdout.write(json.dumps(output_payload, indent=2) + "\n")
