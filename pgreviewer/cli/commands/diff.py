"""Implementation of the ``pgr diff`` command."""

from __future__ import annotations

import asyncio
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

import typer
from rich.console import Console
from rich.table import Table

if TYPE_CHECKING:
    from pgreviewer.core.models import ExtractedQuery, IndexRecommendation, Issue

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


def run_diff(
    diff_file: Path | None,
    json_output: bool,
    only_critical: bool,
    git_ref: str | None = None,
    staged: bool = False,
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

    from pgreviewer.parsing.file_classifier import FileType, classify_file
    from pgreviewer.parsing.sql_extractor_migration import (
        extract_from_alembic_file,
        extract_from_sql_file,
    )
    from pgreviewer.parsing.sql_extractor_raw import extract_raw_sql

    extracted_queries: list[ExtractedQuery] = []
    skipped_files: list[dict[str, str]] = []

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
            if file_type in (FileType.MIGRATION_SQL, FileType.RAW_SQL):
                queries = extract_from_sql_file(local_path)
            elif file_type == FileType.MIGRATION_PYTHON:
                queries = extract_from_alembic_file(local_path)
            elif file_type == FileType.PYTHON_WITH_SQL:
                queries = extract_raw_sql(full_text, file_path=path_str)
            else:
                queries = []

            if not queries:
                skipped_files.append({"file": path_str, "reason": "No SQL found"})
            else:
                extracted_queries.extend(queries)
        except Exception as e:
            skipped_files.append(
                {"file": path_str, "reason": f"Extraction failed: {e}"}
            )

    if not extracted_queries:
        if not json_output:
            console.print("No SQL changes detected.")
        else:
            sys.stdout.write(
                json.dumps({"skipped": skipped_files, "results": []}, indent=2) + "\n"
            )
        return

    # Run Analysis
    try:
        results = asyncio.run(_analyze_all_queries(extracted_queries, only_critical))
    except Exception as exc:
        err_console.print(f"[red]Analysis Error:[/red] {exc}")
        raise typer.Exit(code=1) from None

    if json_output:
        _print_json_diff_report(results, skipped_files)
    else:
        _print_rich_diff_report(results, skipped_files)


async def _analyze_all_queries(
    queries: list[ExtractedQuery], only_critical: bool
) -> list[dict]:
    from pgreviewer.cli.commands.check import _analyse_query

    results = []
    for q in queries:
        issues, recs = await _analyse_query(q.sql)
        if only_critical:
            issues = [i for i in issues if i.severity.value == "CRITICAL"]
            if not issues and not recs:
                # If only critical is requested and it has no critical issues
                # we may skip or just show PASS
                pass

        results.append({"query_obj": q, "issues": issues, "recs": recs})
    return results


def _print_rich_diff_report(results: list[dict], skipped_files: list[dict]) -> None:
    from pgreviewer.cli.commands.check import _print_recommendations

    console.rule("[bold]pgReviewer Diff Analysis[/bold]")

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
            issues: list[Issue] = item["issues"]
            recs: list[IndexRecommendation] = item["recs"]

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


def _print_json_diff_report(results: list[dict], skipped_files: list[dict]) -> None:
    output_results = []
    for item in results:
        q: ExtractedQuery = item["query_obj"]
        issues: list[Issue] = item["issues"]
        recs: list[IndexRecommendation] = item["recs"]

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
            }
        )

    output_payload = {"skipped": skipped_files, "results": output_results}
    sys.stdout.write(json.dumps(output_payload, indent=2) + "\n")
