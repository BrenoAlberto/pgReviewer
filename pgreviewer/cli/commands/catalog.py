from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from pgreviewer.analysis.query_catalog import build_catalog

console = Console()


def run_catalog_build(project_root: Path) -> None:
    catalog = build_catalog(project_root, force_rebuild=True)
    cache_file = project_root.resolve() / ".pgreviewer/query_catalog.json"
    typer.echo(
        f"Catalog built with {len(catalog.functions)} query function(s): {cache_file}"
    )


def run_catalog_show(project_root: Path) -> None:
    catalog = build_catalog(project_root)
    if not catalog.functions:
        typer.echo("No query functions found in catalog.")
        return

    table = Table(title="Query Catalog")
    table.add_column("Function")
    table.add_column("File")
    table.add_column("Line", justify="right")
    table.add_column("Query Method")
    table.add_column("Query Text")

    for function_name in sorted(catalog.functions):
        info = catalog.functions[function_name]
        table.add_row(
            function_name,
            info.file,
            str(info.line),
            info.method_name,
            info.query_text_if_available or "-",
        )
    console.print(table)
