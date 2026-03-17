import importlib.metadata
from pathlib import Path

import typer

app = typer.Typer(help="pgReviewer CLI - Database analysis and optimization tools.")

db_app = typer.Typer(help="Database management commands.")
app.add_typer(db_app, name="db")

debug_app = typer.Typer(help="Debug and diagnostic commands.")
app.add_typer(debug_app, name="debug")


@app.command()
def version() -> None:
    """Print the installed pgreviewer version."""
    ver = importlib.metadata.version("pgreviewer")
    typer.echo(f"pgreviewer {ver}")


@app.command()
def check(
    query: str | None = typer.Argument(None, help="SQL query to analyze"),  # noqa: B008
    query_file: Path | None = typer.Option(  # noqa: B008
        None,
        "--query-file",
        "-f",
        help="Read SQL from a file instead of inline argument.",
    ),
    json_output: bool = typer.Option(  # noqa: B008
        False,
        "--json",
        help="Emit machine-readable JSON instead of a rich report.",
        is_flag=True,
    ),
) -> None:
    """Analyze a SQL query for performance issues."""
    from pgreviewer.cli.commands.check import run_check

    run_check(query=query, query_file=query_file, json_output=json_output)


@app.command(name="check-models")
def check_models_cmd(
    path: Path = typer.Option(  # noqa: B008
        ...,
        "--path",
        help="Path to directory or file containing SQLAlchemy models.",
    ),
    fix: bool = typer.Option(  # noqa: B008
        False,
        "--fix",
        help="Suggest the additions to add.",
        is_flag=True,
    ),
    json_output: bool = typer.Option(  # noqa: B008
        False,
        "--json",
        help="Emit machine-readable JSON instead of a rich report.",
        is_flag=True,
    ),
) -> None:
    """Run static analysis on SQLAlchemy model files."""
    from pgreviewer.cli.commands.check_models import run_check_models

    run_check_models(path=path, fix=fix, json_output=json_output)


@app.command()
def diff(
    diff_file: Path | None = typer.Argument(None, help="Path to the unified diff file"),  # noqa: B008
    git_ref: str | None = typer.Option(  # noqa: B008
        None,
        "--git-ref",
        help=(
            "Run 'git diff <ref>' and analyze the output"
            " (e.g. HEAD~1, main, a commit SHA)."
        ),
    ),
    staged: bool = typer.Option(  # noqa: B008
        False,
        "--staged",
        help=(
            "Run 'git diff --staged' and analyze staged changes"
            " (useful before committing)."
        ),
        is_flag=True,
    ),
    json_output: bool = typer.Option(  # noqa: B008
        False,
        "--json",
        help="Emit machine-readable JSON instead of a rich report.",
        is_flag=True,
    ),
    only_critical: bool = typer.Option(  # noqa: B008
        False,
        "--only-critical",
        help="Suppress INFO and WARNING, only show CRITICAL issues.",
        is_flag=True,
    ),
) -> None:
    """Analyze all SQL queries found in a diff file.

    Provide exactly one input source:

    \b
      pgr diff path/to/changes.patch       # from a diff file
      pgr diff --git-ref HEAD~1            # from last commit
      pgr diff --git-ref main              # from diff against a branch
      pgr diff --staged                    # staged changes (before commit)
    """
    from pgreviewer.cli.commands.diff import run_diff

    run_diff(
        diff_file=diff_file,
        git_ref=git_ref,
        staged=staged,
        json_output=json_output,
        only_critical=only_critical,
    )


@app.command()
def cost(
    month: str | None = typer.Option(
        None, "--month", help="Show spend for a specific month (YYYY-MM)."
    ),
    reset: bool = typer.Option(
        False, "--reset", help="Clear current month's spend.", is_flag=True
    ),
) -> None:
    """Show current month LLM spend, per-category breakdown, and % of budget."""
    from pgreviewer.cli.commands.cost import run_cost

    run_cost(month=month, reset=reset)


@db_app.command("seed")
def db_seed() -> None:
    """Seed the database with realistic data for analysis."""
    typer.echo("Seeding database with realistic data...")
    try:
        from db.seed import run_seed  # db package lives at project root

        run_seed()
        typer.echo("Success: Database seeded and analyzed.")
    except Exception as e:
        typer.echo(f"Error: Seeding failed: {e}", err=True)
        raise typer.Exit(code=1) from e


@debug_app.command("list")
def debug_list() -> None:
    """Tabulate recent runs: date, run_id, query snippet."""
    from pgreviewer.config import settings
    from pgreviewer.infra.debug_store import DebugStore

    store = DebugStore(settings.DEBUG_STORE_PATH)
    runs = store.list_runs()

    if not runs:
        typer.echo("No debug runs found.")
        return

    header = f"{'Date':<12} {'Run ID':<30} {'Query Snippet'}"
    typer.echo(header)
    typer.echo("-" * len(header))
    for run in runs:
        typer.echo(f"{run['date']:<12} {run['run_id']:<30} {run['query_snippet']}")


@debug_app.command("show")
def debug_show(run_id: str) -> None:
    """Pretty-print all stored artifacts for a run."""
    import json

    from pgreviewer.config import settings
    from pgreviewer.infra.debug_store import DebugStore

    store = DebugStore(settings.DEBUG_STORE_PATH)
    try:
        artifacts = store.get_run_artifacts(run_id)
        if not artifacts:
            typer.echo(f"No artifacts found for run {run_id}")
            return

        for category, data in artifacts.items():
            typer.secho(f"\n--- {category} ---", fg=typer.colors.CYAN, bold=True)
            typer.echo(json.dumps(data, indent=2))
    except FileNotFoundError:
        typer.echo(f"Error: Run ID '{run_id}' not found.", err=True)
        raise typer.Exit(code=1) from None


if __name__ == "__main__":
    app()
