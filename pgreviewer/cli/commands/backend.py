import asyncio

import typer

from pgreviewer.config import settings
from pgreviewer.db import pool
from pgreviewer.mcp.client import MCPClient


async def _check_local_db() -> tuple[bool, str]:
    try:
        async with pool.read_session() as conn:
            await conn.fetchval("SELECT 1")
        return True, "reachable"
    except Exception:
        return False, "unreachable (database connectivity check failed)"
    finally:
        await pool.close_pool()


async def _check_mcp_server() -> tuple[bool, str]:
    try:
        async with MCPClient(settings.MCP_SERVER_URL):
            return True, "reachable"
    except Exception:
        return False, "unreachable (MCP connectivity check failed)"


async def _collect_status(backend: str) -> tuple[dict[str, tuple[bool, str]], bool]:
    checks: dict[str, tuple[bool, str]] = {}

    if backend in {"local", "hybrid"}:
        checks["local db"] = await _check_local_db()

    if backend in {"mcp", "hybrid"}:
        checks["mcp server"] = await _check_mcp_server()

    return checks, all(ok for ok, _ in checks.values())


def run_backend_status() -> None:
    backend = settings.BACKEND.lower()
    if backend not in {"local", "mcp", "hybrid"}:
        typer.echo(
            f"Unsupported BACKEND '{settings.BACKEND}'. "
            "Expected one of: local, mcp, hybrid.",
            err=True,
        )
        raise typer.Exit(code=1)

    typer.echo(f"Configured backend: {backend}")
    checks, is_ok = asyncio.run(_collect_status(backend))

    for name, (ok, detail) in checks.items():
        prefix = "[OK]" if ok else "[FAIL]"
        typer.echo(f"{prefix} {name}: {detail}")

    if is_ok:
        typer.echo("Backend status: ready.")
        return

    typer.echo("Backend status: unavailable dependencies detected.", err=True)
    raise typer.Exit(code=1)
