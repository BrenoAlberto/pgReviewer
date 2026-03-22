from unittest.mock import AsyncMock, patch

from typer.testing import CliRunner

from pgreviewer.cli.main import app


def test_backend_status_local_success() -> None:
    runner = CliRunner()
    with patch(
        "pgreviewer.cli.commands.backend._check_local_db",
        AsyncMock(return_value=(True, "reachable")),
    ):
        result = runner.invoke(app, ["backend", "status"])

    assert result.exit_code == 0
    assert "Configured backend: local" in result.output
    assert "[OK] local db: reachable" in result.output
    assert "Backend status: ready." in result.output


def test_backend_status_local_unavailable() -> None:
    runner = CliRunner()
    with patch(
        "pgreviewer.cli.commands.backend._check_local_db",
        AsyncMock(
            return_value=(False, "unreachable (database connectivity check failed)")
        ),
    ):
        result = runner.invoke(app, ["backend", "status"])

    assert result.exit_code == 1
    assert (
        "[FAIL] local db: unreachable (database connectivity check failed)"
        in result.output
    )
    assert "Backend status: unavailable dependencies detected." in result.output
