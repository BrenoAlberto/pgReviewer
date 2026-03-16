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


@app.command()
def diff(
    diff_file: Path = typer.Argument(..., help="Path to the unified diff file"),  # noqa: B008
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
    """Analyze all SQL queries found in a diff file."""
    from pgreviewer.cli.commands.diff import run_diff

    run_diff(diff_file=diff_file, json_output=json_output, only_critical=only_critical)


@app.command()
def cost() -> None:
    """Show current month LLM spend, per-category breakdown, and % of budget."""
    from pgreviewer.config import Settings
    from pgreviewer.infra.cost_guardrail import CostGuardrail

    s = Settings()
    guardrail = CostGuardrail(
        cost_store_path=s.COST_STORE_PATH,
        monthly_budget_usd=s.LLM_MONTHLY_BUDGET_USD,
        category_limits=s.LLM_CATEGORY_LIMITS,
        cost_per_token=s.LLM_COST_PER_TOKEN,
    )
    rows = guardrail.month_summary()

    header = f"{'Category':<15} {'Spent ($)':>12} {'Limit ($)':>12} {'Used (%)':>10}"
    typer.echo(header)
    typer.echo("-" * len(header))
    total_spent = 0.0
    total_limit = 0.0
    for r in rows:
        cat, spent, lim, pct = r["category"], r["spent"], r["limit"], r["pct"]
        typer.echo(f"{cat:<15} {spent:>12.4f} {lim:>12.4f} {pct:>9.1f}%")
        total_spent += spent  # type: ignore[operator]
        total_limit += lim  # type: ignore[operator]
    typer.echo("-" * len(header))
    total_pct = (total_spent / total_limit * 100) if total_limit > 0 else 0.0
    typer.echo(
        f"{'TOTAL':<15} {total_spent:>12.4f} {total_limit:>12.4f} {total_pct:>9.1f}%"
    )


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
