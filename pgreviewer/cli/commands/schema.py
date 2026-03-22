"""Implementation of the ``pgr schema dump`` command."""

from __future__ import annotations

import asyncio

import typer
from rich.console import Console

err_console = Console(stderr=True)


def run_schema_dump(output: str, *, no_stats: bool) -> None:
    """Dump the database schema (DDL + statistics) to a file."""
    from pathlib import Path

    from pgreviewer.config import settings
    from pgreviewer.exceptions import SchemaDumpError

    if not settings.DATABASE_URL:
        err_console.print(
            "[red]Error:[/red] DATABASE_URL is not set. "
            "Set it in your environment or .env file."
        )
        raise typer.Exit(code=1)

    database_url = str(settings.DATABASE_URL)
    output_path = Path(output)

    try:
        from pgreviewer.analysis.schema_dumper import dump_schema

        asyncio.run(dump_schema(database_url, output_path, no_stats=no_stats))
    except SchemaDumpError as exc:
        err_console.print(f"[red]Schema dump error:[/red] {exc}")
        raise typer.Exit(code=1) from None
    except Exception as exc:
        err_console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(code=1) from None

    size_kb = output_path.stat().st_size / 1024
    typer.echo(f"Schema dumped to {output_path} ({size_kb:.1f} KB)")
    if no_stats:
        typer.echo("Stats collection skipped (--no-stats).")
