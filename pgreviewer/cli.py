import click

from db.seed import run_seed


@click.group()
def cli():
    """pgReviewer CLI - Database analysis and optimization tools."""
    pass


@cli.group()
def db():
    """Database management commands."""
    pass


@db.command()
def seed():
    """Seed the database with realistic data for analysis."""
    click.echo("Seeding database with realistic data...")
    try:
        run_seed()
        click.echo("Success: Database seeded and analyzed.")
    except Exception as e:
        click.echo(f"Error: Seeding failed: {e}", err=True)
        raise click.Abort() from e


if __name__ == "__main__":
    cli()
