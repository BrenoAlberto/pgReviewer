import json
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from pgreviewer.cli.main import app

runner = CliRunner()


def test_cost_command_table_output(tmp_path: Path):
    cost_dir = tmp_path / "costs"
    cost_dir.mkdir()

    # Create a mock cost file for the current month
    from datetime import UTC, datetime

    now = datetime.now(tz=UTC)
    current_month = now.strftime("%Y-%m")
    cost_file = cost_dir / f"{current_month}.json"

    cost_data = {
        "interpretation": {"spent": 1.23, "calls": 10},
        "extraction": {"spent": 0.45, "calls": 5},
    }
    cost_file.write_text(json.dumps(cost_data))

    with (
        patch("pgreviewer.config.settings.COST_STORE_PATH", cost_dir),
        patch("pgreviewer.config.settings.LLM_MONTHLY_BUDGET_USD", 10.0),
        patch("pgreviewer.config.settings.LLM_BUDGET_INTERPRETATION", 0.5),
        patch("pgreviewer.config.settings.LLM_BUDGET_EXTRACTION", 0.3),
        patch("pgreviewer.config.settings.LLM_BUDGET_REPORTING", 0.2),
    ):
        result = runner.invoke(app, ["cost"])

        assert result.exit_code == 0
        assert "interpretation" in result.output
        assert "$1.23" in result.output
        assert "$5.00" in result.output  # 50% of 10.0
        assert "24.6%" in result.output  # 1.23 / 5.0
        assert "10" in result.output

        assert "extraction" in result.output
        assert "$0.45" in result.output
        assert "$3.00" in result.output  # 30% of 10.0
        assert "15.0%" in result.output  # 0.45 / 3.0
        assert "5" in result.output

        assert "total" in result.output
        assert "$1.68" in result.output  # 1.23 + 0.45
        assert "15" in result.output


def test_cost_command_specific_month(tmp_path: Path):
    cost_dir = tmp_path / "costs"
    cost_dir.mkdir()

    cost_file = cost_dir / "2024-01.json"
    cost_data = {"interpretation": {"spent": 2.0, "calls": 20}}
    cost_file.write_text(json.dumps(cost_data))

    with patch("pgreviewer.config.settings.COST_STORE_PATH", cost_dir):
        result = runner.invoke(app, ["cost", "--month", "2024-01"])

        assert result.exit_code == 0
        assert "LLM Spend Breakdown (2024-01)" in result.output
        assert "$2.00" in result.output
        assert "20" in result.output


def test_cost_command_reset(tmp_path: Path):
    cost_dir = tmp_path / "costs"
    cost_dir.mkdir()

    from datetime import UTC, datetime

    now = datetime.now(tz=UTC)
    current_month = now.strftime("%Y-%m")
    cost_file = cost_dir / f"{current_month}.json"
    cost_file.write_text("{}")

    assert cost_file.exists()

    with patch("pgreviewer.config.settings.COST_STORE_PATH", cost_dir):
        # Test reset with 'y' confirmation
        result = runner.invoke(app, ["cost", "--reset"], input="y\n")

        assert result.exit_code == 0
        assert "Spend data cleared." in result.output
        assert not cost_file.exists()
