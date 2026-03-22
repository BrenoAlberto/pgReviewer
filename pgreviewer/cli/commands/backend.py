import asyncio

import typer

from pgreviewer.db import pool


async def _check_local_db() -> tuple[bool, str]:
    try:
        async with pool.read_session() as conn:
            await conn.fetchval("SELECT 1")
        return True, "reachable"
    except Exception:
        return False, "unreachable (database connectivity check failed)"
    finally:
        await pool.close_pool()


def run_backend_status() -> None:
    typer.echo("Configured backend: local")
    ok, detail = asyncio.run(_check_local_db())
    prefix = "[OK]" if ok else "[FAIL]"
    typer.echo(f"{prefix} local db: {detail}")

    if ok:
        typer.echo("Backend status: ready.")
        return

    typer.echo("Backend status: unavailable dependencies detected.", err=True)
    raise typer.Exit(code=1)
