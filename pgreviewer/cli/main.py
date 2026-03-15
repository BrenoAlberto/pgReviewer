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


if __name__ == "__main__":
    app()
