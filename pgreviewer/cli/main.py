import importlib.metadata
from pathlib import Path
from typing import Literal

import typer

app = typer.Typer(help="pgReviewer CLI - Database analysis and optimization tools.")

db_app = typer.Typer(help="Database management commands.")
app.add_typer(db_app, name="db")

backend_app = typer.Typer(help="Backend connectivity commands.")
app.add_typer(backend_app, name="backend")

debug_app = typer.Typer(help="Debug and diagnostic commands.")
app.add_typer(debug_app, name="debug")
catalog_app = typer.Typer(help="Query catalog commands.")
app.add_typer(catalog_app, name="catalog")
config_app = typer.Typer(help="Project config commands.")
app.add_typer(config_app, name="config")
schema_app = typer.Typer(help="Schema export commands.")
app.add_typer(schema_app, name="schema")


@app.command(name="detect-pg-version")
def detect_pg_version(
    path: Path = typer.Option(  # noqa: B008
        Path("."),
        "--path",
        help="Directory to scan (default: current directory).",
    ),
) -> None:
    """Detect the Postgres major version used by this project.

    Scans docker-compose files and Dockerfiles for postgres image references
    and prints the detected major version. Exits with code 0 and prints the
    version number (e.g. '15'). Prints nothing and exits 1 if no version is
    detected, so callers can fall back to a default.
    """
    from pgreviewer.ci.pg_version_detector import detect

    version = detect(path)
    if version is None:
        raise typer.Exit(code=1)
    typer.echo(str(version))


@app.command(name="detect-extensions")
def detect_extensions(
    path: Path = typer.Option(  # noqa: B008
        Path("."),
        "--path",
        help="Directory to scan for migration files (default: current directory).",
    ),
    apt_packages: bool = typer.Option(  # noqa: B008
        False,
        "--apt-packages",
        help="Print only apt package names (space-separated), for scripting.",
        is_flag=True,
    ),
    postgres_version: int = typer.Option(  # noqa: B008
        16,
        "--postgres-version",
        help="Postgres major version to target for apt package names (default: 16).",
    ),
) -> None:
    """Detect Postgres extensions required by migrations and map to apt packages.

    Exits with code 1 if any extension has no known package mapping, printing
    a message that points to the db-dockerfile-context escape hatch.
    """
    from pgreviewer.ci.extension_detector import BUNDLED_EXTENSIONS, detect

    result = detect(path, pg_version=postgres_version)

    if result.unknown_extensions:
        unknown_list = "\n".join(f"  - {e}" for e in result.unknown_extensions)
        typer.echo(
            "::error::pgReviewer detected extensions with no known apt package"
            f" mapping:\n{unknown_list}\n\nProvide a custom 'db-dockerfile-context'"
            " directory whose Dockerfile installs these extensions on top of the"
            " pgReviewer base image.",
            err=True,
        )
        raise typer.Exit(code=1)

    if apt_packages:
        typer.echo(" ".join(result.packages_to_install))
    else:
        if result.extensions_found:
            bundled = (
                ", ".join(
                    e
                    for e in sorted(result.extensions_found)
                    if e in BUNDLED_EXTENSIONS
                )
                or "none"
            )
            typer.echo(
                f"Extensions found:    {', '.join(sorted(result.extensions_found))}"
            )
            typer.echo(f"Already bundled:     {bundled}")
            typer.echo(
                "Packages to install: "
                f"{', '.join(result.packages_to_install) or 'none'}"
            )
        else:
            typer.echo("No CREATE EXTENSION statements found in migration files.")


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
    verbose: bool = typer.Option(  # noqa: B008
        False,
        "--verbose",
        help="Show full EXPLAIN JSON and detailed issue interpretation.",
        is_flag=True,
    ),
    no_color: bool = typer.Option(  # noqa: B008
        False,
        "--no-color",
        help="Disable ANSI colors for plain-text output.",
        is_flag=True,
    ),
) -> None:
    """Analyze a SQL query for performance issues."""
    from pgreviewer.cli.commands.check import run_check

    run_check(
        query=query,
        query_file=query_file,
        json_output=json_output,
        verbose=verbose,
        no_color=no_color,
    )


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
    ci: bool = typer.Option(  # noqa: B008
        False,
        "--ci",
        help="CI mode: exit 1 when the severity threshold is violated, else exit 0.",
        is_flag=True,
    ),
    severity_threshold: Literal["critical", "warning", "info", "none"] = typer.Option(  # noqa: B008
        "critical",
        "--severity-threshold",
        help="Fail threshold for --ci mode: critical, warning, info, or none.",
    ),
    config: Path | None = typer.Option(  # noqa: B008
        None,
        "--config",
        help=(
            "Path to a .pgreviewer.yml config file (overrides auto-discovery from CWD)."
        ),
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
        ci=ci,
        severity_threshold=severity_threshold,
        config=config,
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


@app.command()
def workload(
    top: int = typer.Option(20, "--top", help="Show the top N slow queries.", min=1),  # noqa: B008
    min_calls: int = typer.Option(  # noqa: B008
        0,
        "--min-calls",
        help="Only include queries called more than N times/day.",
        min=0,
    ),
    export: Literal["markdown"] | None = typer.Option(  # noqa: B008
        None,
        "--export",
        help="Export output format (markdown).",
    ),
) -> None:
    """Analyze top slow queries from pg_stat_statements and suggest indexes."""
    from pgreviewer.cli.commands.workload import run_workload

    run_workload(top=top, min_calls=min_calls, export=export)


@catalog_app.command("build")
def catalog_build(
    project_root: Path = typer.Option(Path("."), "--project-root", help="Project root"),  # noqa: B008
) -> None:
    """Build query-function catalog from python source files."""
    from pgreviewer.cli.commands.catalog import run_catalog_build

    run_catalog_build(project_root=project_root)


@catalog_app.command("show")
def catalog_show(
    project_root: Path = typer.Option(Path("."), "--project-root", help="Project root"),  # noqa: B008
) -> None:
    """Display query-function catalog."""
    from pgreviewer.cli.commands.catalog import run_catalog_show

    run_catalog_show(project_root=project_root)


@config_app.command("init")
def config_init(
    config: Path = typer.Option(  # noqa: B008
        Path(".pgreviewer.yml"),
        "--config",
        help="Path to config file",
    ),
) -> None:
    """Create a fully commented .pgreviewer.yml in the current directory."""
    from pgreviewer.cli.commands.config import run_config_init

    run_config_init(path=config)


@config_app.command("validate")
def config_validate(
    config: Path = typer.Option(  # noqa: B008
        Path(".pgreviewer.yml"),
        "--config",
        help="Path to config file",
    ),
) -> None:
    """Validate .pgreviewer.yml in the current directory."""
    from pgreviewer.cli.commands.config import run_config_validate

    run_config_validate(path=config)


@schema_app.command("dump")
def schema_dump(
    output: str = typer.Option(  # noqa: B008
        ".pgreviewer/schema.sql",
        "--output",
        "-o",
        help="Output file path.",
    ),
    no_stats: bool = typer.Option(  # noqa: B008
        False,
        "--no-stats",
        help="Skip stats collection, dump DDL only.",
        is_flag=True,
    ),
) -> None:
    """Export database schema and statistics for offline analysis.

    Runs ``pg_dump --schema-only`` for DDL and collects table/column
    statistics from ``pg_stats``.  The output file can be committed to
    source control so that pgReviewer can run schema-aware analysis
    without a live database connection in CI.
    """
    from pgreviewer.cli.commands.schema import run_schema_dump

    run_schema_dump(output, no_stats=no_stats)


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


@backend_app.command("status")
def backend_status() -> None:
    """Show configured backend and verify required connectivity."""
    from pgreviewer.cli.commands.backend import run_backend_status

    run_backend_status()


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
