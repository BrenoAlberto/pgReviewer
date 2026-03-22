from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from pgreviewer.core.models import ExplainPlan, Issue, PlanNode, Severity
from pgreviewer.exceptions import LLMUnavailableError

runner = CliRunner()


@pytest.mark.asyncio
async def test_analyse_query_degradation_llm_unavailable(monkeypatch):
    """_analyse_query catches LLMUnavailableError and sets degraded status."""
    from pgreviewer.cli.commands.check import _analyse_query
    from pgreviewer.config import settings

    monkeypatch.setattr(settings, "LLM_API_KEY", "test-key")
    monkeypatch.setattr(settings, "DATABASE_URL", "postgresql://u:p@localhost/d")

    with (
        patch("pgreviewer.db.pool.read_session") as mock_read,
        patch("pgreviewer.analysis.explain_runner.run_explain") as mock_explain,
        patch("pgreviewer.analysis.plan_parser.parse_explain") as mock_parse,
        patch("pgreviewer.analysis.plan_parser.extract_tables", return_value=[]),
        patch("pgreviewer.analysis.issue_detectors.run_all_detectors") as mock_detect,
        patch("pgreviewer.infra.debug_store.DebugStore") as mock_store_cls,
        patch("pgreviewer.llm.client.LLMClient") as mock_client_cls,
        patch("pgreviewer.db.pool.close_pool"),
    ):
        mock_store_cls.return_value.new_run_id.return_value = "run-1"
        mock_client_cls.return_value.generate.side_effect = LLMUnavailableError(
            "Service Down"
        )

        mock_read.return_value.__aenter__.return_value = None
        mock_explain.return_value = {}
        mock_parse.return_value = ExplainPlan(
            root=PlanNode(
                node_type="Nested Loop",
                total_cost=100.0,
                startup_cost=1.0,
                plan_rows=1000,
                plan_width=64,
                children=[
                    PlanNode(
                        node_type="Nested Loop",
                        total_cost=50.0,
                        startup_cost=1.0,
                        plan_rows=500,
                        plan_width=32,
                    ),
                    PlanNode(
                        node_type="Nested Loop",
                        total_cost=30.0,
                        startup_cost=1.0,
                        plan_rows=200,
                        plan_width=32,
                    ),
                ],
            )
        )
        mock_detect.return_value = [
            Issue(Severity.WARNING, "alg", "desc", None, [], "action")
        ]

        result = await _analyse_query("SELECT 1")

        assert result.llm_degraded is True
        assert "Service Down" in result.degradation_reason
        assert len(result.issues) == 1
        assert result.issues[0].detector_name == "alg"
        mock_store_cls.return_value.save.assert_called_once()


def test_run_check_shows_degradation_notice(capsys):
    """CLI shows warning notice when LLM is degraded."""
    from pgreviewer.cli.commands.check import run_check
    from pgreviewer.core.degradation import AnalysisResult

    result = AnalysisResult(
        issues=[Issue(Severity.WARNING, "alg", "desc", None, [], "action")],
        llm_degraded=True,
        degradation_reason="Budget Exceeded",
    )

    with (
        patch("pgreviewer.cli.commands.check._analyse_query", return_value=result),
        patch("pgreviewer.cli.commands.check.asyncio.run", return_value=result),
    ):
        run_check(query="SELECT 1", query_file=None, json_output=False)

        captured = capsys.readouterr()
        assert "⚠️  Budget Exceeded — showing algorithmic analysis only" in captured.out
        assert "alg" in captured.out


def test_diff_shows_degradation_notice_per_query(tmp_path, monkeypatch):
    """pgr diff shows degradation notice when analysis fails for a query."""
    from pgreviewer.config import settings
    from pgreviewer.core.degradation import AnalysisResult

    # Create a dummy diff file
    diff_file = tmp_path / "test.diff"
    diff_file.write_text(
        "--- a/f.sql\n+++ b/f.sql\n@@ -1 +1 @@\n-SELECT 1;\n+SELECT 2;"
    )

    # Create a dummy f.sql on disk
    (tmp_path / "f.sql").write_text("SELECT 2;")

    result = AnalysisResult(issues=[], llm_degraded=True, degradation_reason="LLM Down")

    monkeypatch.setattr(settings, "DATABASE_URL", "postgresql://u:p@localhost/d")

    with (
        patch("pgreviewer.cli.commands.diff.Path.cwd", return_value=tmp_path),
        patch("pgreviewer.cli.commands.diff._analyze_all_queries") as mock_analyze,
    ):
        from pgreviewer.core.models import ExtractedQuery

        q = ExtractedQuery("SELECT 2", "f.sql", 1, "raw", 1.0)
        mock_analyze.return_value = [
            {"query_obj": q, "analysis_result": result, "issues": result.issues}
        ]

        # Using runner to capture output is better for Typer commands
        from typer.testing import CliRunner

        from pgreviewer.cli.main import app

        runner = CliRunner()

        # We need to be careful with paths here
        import os

        os.chdir(tmp_path)
        res = runner.invoke(app, ["diff", "test.diff"])

        assert res.exit_code == 0
        assert "⚠️  LLM Down — showing algorithmic analysis only" in res.output
