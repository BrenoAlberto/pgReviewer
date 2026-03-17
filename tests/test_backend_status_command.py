from unittest.mock import AsyncMock, patch

from typer.testing import CliRunner

from pgreviewer.cli.main import app

runner = CliRunner()


def test_backend_status_hybrid_checks_local_and_mcp_success() -> None:
    with (
        patch("pgreviewer.cli.commands.backend.settings.BACKEND", "hybrid"),
        patch(
            "pgreviewer.cli.commands.backend._check_local_db",
            AsyncMock(return_value=(True, "reachable")),
        ),
        patch(
            "pgreviewer.cli.commands.backend._check_mcp_server",
            AsyncMock(return_value=(True, "reachable")),
        ),
    ):
        result = runner.invoke(app, ["backend", "status"])

    assert result.exit_code == 0
    assert "Configured backend: hybrid" in result.output
    assert "[OK] local db: reachable" in result.output
    assert "[OK] mcp server: reachable" in result.output
    assert "Backend status: ready." in result.output


def test_backend_status_hybrid_fails_when_local_is_unavailable() -> None:
    with (
        patch("pgreviewer.cli.commands.backend.settings.BACKEND", "hybrid"),
        patch(
            "pgreviewer.cli.commands.backend._check_local_db",
            AsyncMock(return_value=(False, "unreachable (db down)")),
        ),
        patch(
            "pgreviewer.cli.commands.backend._check_mcp_server",
            AsyncMock(return_value=(True, "reachable")),
        ),
    ):
        result = runner.invoke(app, ["backend", "status"])

    assert result.exit_code == 1
    assert "[FAIL] local db: unreachable (db down)" in result.output
    assert "[OK] mcp server: reachable" in result.output
    assert "Backend status: unavailable dependencies detected." in result.output


def test_backend_status_mcp_only_checks_mcp() -> None:
    with (
        patch("pgreviewer.cli.commands.backend.settings.BACKEND", "mcp"),
        patch(
            "pgreviewer.cli.commands.backend._check_mcp_server",
            AsyncMock(return_value=(True, "reachable")),
        ),
    ):
        result = runner.invoke(app, ["backend", "status"])

    assert result.exit_code == 0
    assert "Configured backend: mcp" in result.output
    assert "[OK] mcp server: reachable" in result.output
    assert "local db" not in result.output
