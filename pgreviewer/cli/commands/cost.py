import typer
from rich.console import Console
from rich.table import Table

from pgreviewer.config import settings
from pgreviewer.infra.cost_guardrail import CostGuardrail

console = Console()


def run_cost(
    month: str | None = typer.Option(
        None, "--month", help="Show spend for a specific month (YYYY-MM)."
    ),
    reset: bool = typer.Option(
        False, "--reset", help="Clear current month's spend.", is_flag=True
    ),
) -> None:
    """Show LLM spend breakdown by category and month."""
    guardrail = CostGuardrail(
        cost_store_path=settings.COST_STORE_PATH,
        monthly_budget_usd=settings.LLM_MONTHLY_BUDGET_USD,
        category_limits=settings.llm_category_limits,
    )

    if reset:
        m = month or "the current month"
        msg = f"Are you sure you want to clear spend data for {m}?"
        confirm = typer.confirm(msg)
        if confirm:
            guardrail.reset(month)
            typer.echo("Spend data cleared.")
        return

    rows = guardrail.month_summary(month)

    table = Table(title=f"LLM Spend Breakdown ({month or 'current month'})")
    table.add_column("Category", style="cyan")
    table.add_column("Spent (USD)", justify="right")
    table.add_column("Budget (USD)", justify="right")
    table.add_column("% Used", justify="right")
    table.add_column("Calls", justify="right")

    total_spent = 0.0
    total_limit = 0.0
    total_calls = 0

    for r in rows:
        cat = r["category"]
        spent = float(r["spent"])
        limit = float(r["limit"])
        pct = float(r["pct"])
        calls = int(r["calls"])

        table.add_row(
            str(cat),
            f"${spent:.2f}",
            f"${limit:.2f}",
            f"{pct:.1f}%",
            str(calls),
        )

        total_spent += spent
        total_limit += limit
        total_calls += calls

    total_pct = (total_spent / total_limit * 100) if total_limit > 0 else 0.0

    table.add_section()
    table.add_row(
        "[bold]total[/bold]",
        f"[bold]${total_spent:.2f}[/bold]",
        f"[bold]${total_limit:.2f}[/bold]",
        f"[bold]{total_pct:.1f}%[/bold]",
        f"[bold]{total_calls}[/bold]",
    )

    console.print(table)
