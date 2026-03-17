from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

from typer.testing import CliRunner

from pgreviewer.cli.main import app
from pgreviewer.core.degradation import AnalysisResult
from pgreviewer.core.models import IndexRecommendation, Issue, Severity, SlowQuery

runner = CliRunner()


def _make_slow_query(query: str, calls: int, mean_ms: float) -> SlowQuery:
    return SlowQuery(
        query_text=query,
        calls=calls,
        mean_exec_time_ms=mean_ms,
        total_exec_time_ms=mean_ms * calls,
        rows=100,
    )


def test_workload_command_wiring_passes_cli_options() -> None:
    with patch("pgreviewer.cli.commands.workload.run_workload") as mock_run:
        result = runner.invoke(
            app,
            ["workload", "--top", "5", "--min-calls", "100", "--export", "markdown"],
        )

    assert result.exit_code == 0
    mock_run.assert_called_once_with(top=5, min_calls=100, export="markdown")


def test_run_workload_filters_by_min_calls(capsys) -> None:
    from pgreviewer.cli.commands.workload import WorkloadQueryAnalysis, run_workload

    slow_queries = [
        _make_slow_query("SELECT * FROM users", calls=90, mean_ms=100.0),
        _make_slow_query("SELECT * FROM orders", calls=120, mean_ms=200.0),
    ]
    analyzed_rows = [
        WorkloadQueryAnalysis(
            query_fingerprint="SELECT * FROM orders",
            calls_per_day=120,
            avg_time_ms=200.0,
            issues_found=2,
            top_recommendation="CREATE INDEX idx_orders_user_id ON orders(user_id)",
        )
    ]
    with (
        patch(
            "pgreviewer.cli.commands.workload._fetch_slow_queries",
            AsyncMock(return_value=slow_queries),
        ),
        patch(
            "pgreviewer.cli.commands.workload._analyze_slow_queries",
            AsyncMock(return_value=analyzed_rows),
        ) as mock_analyze,
    ):
        run_workload(top=5, min_calls=100, export=None)

    mock_analyze.assert_awaited_once_with([slow_queries[1]])
    output = capsys.readouterr().out
    assert "120" in output
    assert "200.00" in output


def test_run_workload_markdown_export_outputs_table(capsys) -> None:
    from pgreviewer.cli.commands.workload import WorkloadQueryAnalysis, run_workload

    slow_queries = [_make_slow_query("SELECT * FROM users WHERE id = $1", 200, 123.4)]
    rows = [
        WorkloadQueryAnalysis(
            query_fingerprint="SELECT * FROM users WHERE id = $1",
            calls_per_day=200,
            avg_time_ms=123.4,
            issues_found=1,
            top_recommendation="CREATE INDEX idx_users_id ON users(id)",
        )
    ]
    with (
        patch(
            "pgreviewer.cli.commands.workload._fetch_slow_queries",
            AsyncMock(return_value=slow_queries),
        ),
        patch(
            "pgreviewer.cli.commands.workload._analyze_slow_queries",
            AsyncMock(return_value=rows),
        ),
    ):
        run_workload(top=1, min_calls=0, export="markdown")

    output = capsys.readouterr().out
    assert output.splitlines()[0] == (
        "| Rank | Query fingerprint | Calls/day | Avg time (ms) | Issues found | "
        "Top recommendation |"
    )
    assert (
        "| 1 | SELECT * FROM users WHERE id = $1 | 200 | 123.40 | 1 | "
        "CREATE INDEX idx_users_id ON users(id) |" in output
    )


def test_analyze_slow_queries_uses_pipeline_results() -> None:
    from pgreviewer.cli.commands.workload import _analyze_slow_queries

    slow_query = _make_slow_query(
        "SELECT * FROM orders WHERE user_id = $1 ORDER BY created_at DESC",
        calls=500,
        mean_ms=80.0,
    )
    analysis_result = AnalysisResult(
        issues=[
            Issue(
                severity=Severity.WARNING,
                detector_name="missing_index",
                description="desc",
                affected_table="orders",
                affected_columns=["user_id"],
                suggested_action="add index",
            )
        ],
        recommendations=[
            IndexRecommendation(
                table="orders",
                columns=["user_id", "created_at"],
                create_statement=(
                    "CREATE INDEX idx_orders_user_created ON "
                    "orders(user_id, created_at DESC)"
                ),
                improvement_pct=0.6,
            )
        ],
    )

    with patch(
        "pgreviewer.cli.commands.workload._analyse_query",
        AsyncMock(return_value=analysis_result),
    ):
        rows = asyncio.run(_analyze_slow_queries([slow_query]))

    assert len(rows) == 1
    assert rows[0].issues_found == 1
    assert rows[0].query_fingerprint.startswith("SELECT * FROM orders")
    assert rows[0].top_recommendation.startswith("CREATE INDEX idx_orders_user_created")
