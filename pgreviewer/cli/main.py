import importlib.metadata

import typer

app = typer.Typer(help="pgReviewer CLI - Database analysis and optimization tools.")

db_app = typer.Typer(help="Database management commands.")
app.add_typer(db_app, name="db")


@app.command()
def version() -> None:
    """Print the installed pgreviewer version."""
    ver = importlib.metadata.version("pgreviewer")
    typer.echo(f"pgreviewer {ver}")


@app.command()
def check() -> None:
    """Analyze query plans and surface slow or inefficient queries."""
    typer.echo("Not implemented yet")


@app.command()
def diff() -> None:
    """Compare query performance between two schema or data states."""
    typer.echo("Not implemented yet")


@app.command()
def cost() -> None:
    """Estimate the cost of running a query or migration."""
    typer.echo("Not implemented yet")


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


if __name__ == "__main__":
    app()
