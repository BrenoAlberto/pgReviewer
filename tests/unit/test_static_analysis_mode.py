"""Tests for static-only analysis mode (no DATABASE_URL)."""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from pgreviewer.cli.main import app
from pgreviewer.config import Settings
from pgreviewer.core.models import AnalysisMode

if TYPE_CHECKING:
    from pathlib import Path

runner = CliRunner()


# ---------------------------------------------------------------------------
# AnalysisMode detection
# ---------------------------------------------------------------------------


def test_analysis_mode_static_when_no_database_url():
    s = Settings(DATABASE_URL=None, _env_file=None)
    assert s.analysis_mode == AnalysisMode.STATIC_ONLY


def test_analysis_mode_full_when_database_url_set():
    s = Settings(
        DATABASE_URL="postgresql://u:p@localhost:5432/db",
        _env_file=None,
    )
    assert s.analysis_mode == AnalysisMode.FULL


def test_empty_database_url_string_becomes_none():
    s = Settings(DATABASE_URL="", _env_file=None)
    assert s.DATABASE_URL is None
    assert s.analysis_mode == AnalysisMode.STATIC_ONLY


# ---------------------------------------------------------------------------
# has_llm property
# ---------------------------------------------------------------------------


def test_has_llm_false_when_no_keys():
    s = Settings(DATABASE_URL=None, _env_file=None)
    assert s.has_llm is False


def test_has_llm_true_with_anthropic_key():
    s = Settings(
        DATABASE_URL=None,
        ANTHROPIC_API_KEY="sk-test",
        _env_file=None,
    )
    assert s.has_llm is True


def test_has_llm_false_when_disabled():
    s = Settings(
        DATABASE_URL=None,
        ANTHROPIC_API_KEY="sk-test",
        LLM_DISABLED=True,
        _env_file=None,
    )
    assert s.has_llm is False


# ---------------------------------------------------------------------------
# Static-only diff analysis (end-to-end via CLI)
# ---------------------------------------------------------------------------

_MIGRATION_DIFF = """\
--- /dev/null
+++ b/migrations/0001_add_orders.sql
@@ -0,0 +1,3 @@
+CREATE TABLE orders (id SERIAL PRIMARY KEY);
+CREATE INDEX idx_orders_user ON orders(user_id);
+ALTER TABLE users ADD COLUMN age INTEGER NOT NULL;
"""


@pytest.fixture()
def migration_on_disk(tmp_path: Path) -> Path:
    """Write the migration file so diff analysis can read it."""
    mig_dir = tmp_path / "migrations"
    mig_dir.mkdir()
    mig_file = mig_dir / "0001_add_orders.sql"
    mig_file.write_text(
        "CREATE TABLE orders (id SERIAL PRIMARY KEY);\n"
        "CREATE INDEX idx_orders_user ON orders(user_id);\n"
        "ALTER TABLE users ADD COLUMN age INTEGER NOT NULL;\n"
    )
    diff_file = tmp_path / "test.diff"
    diff_file.write_text(_MIGRATION_DIFF)
    return tmp_path


def test_static_diff_finds_migration_issues(migration_on_disk: Path):
    """pgr diff produces findings in static mode (no DB, no LLM)."""
    os.chdir(migration_on_disk)

    with patch.dict(os.environ, {"DATABASE_URL": ""}, clear=False):
        result = runner.invoke(app, ["diff", "test.diff", "--json"])

    assert result.exit_code == 0, result.output
    data = json.loads(result.output)

    issues = [issue for r in data["results"] for issue in r["issues"]]
    detector_names = {i["detector_name"] for i in issues}

    # Static detectors should fire
    assert "create_index_not_concurrently" in detector_names
    assert "add_not_null_without_default" in detector_names

    # No LLM or DB should have been used
    assert data["metadata"]["llm_used"] is False


def test_static_diff_ci_mode_exits_correctly(migration_on_disk: Path):
    """CI mode works in static-only mode."""
    os.chdir(migration_on_disk)

    with patch.dict(os.environ, {"DATABASE_URL": ""}, clear=False):
        result = runner.invoke(
            app, ["diff", "test.diff", "--ci", "--severity-threshold", "warning"]
        )

    # Should exit 1 because there are WARNING-level findings
    assert result.exit_code == 1


def test_static_mode_skips_explain_analysis(migration_on_disk: Path):
    """In static mode, _analyse_query_with_config is never called."""
    os.chdir(migration_on_disk)

    with (
        patch.dict(os.environ, {"DATABASE_URL": ""}, clear=False),
        patch(
            "pgreviewer.cli.commands.check._analyse_query_with_config"
        ) as mock_explain,
    ):
        result = runner.invoke(app, ["diff", "test.diff", "--json"])

    assert result.exit_code == 0, result.output
    mock_explain.assert_not_called()
