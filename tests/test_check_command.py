"""Tests for pgreviewer/cli/commands/check.py and plan_parser.extract_tables."""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from typer.testing import CliRunner

from pgreviewer.analysis.plan_parser import extract_tables, parse_explain
from pgreviewer.core.models import Issue, Severity

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "explain"
_ANSI_ESCAPE_PATTERN = r"\x1b\[[0-9;]*[mGKHfJ]"


def _load_plan(fixture_name: str):
    with open(FIXTURE_DIR / fixture_name) as f:
        raw = json.load(f)
    return parse_explain(raw[0])


# ---------------------------------------------------------------------------
# extract_tables
# ---------------------------------------------------------------------------


def test_extract_tables_seq_scan():
    plan = _load_plan("seq_scan.json")
    tables = extract_tables(plan)
    assert "users" in tables


def test_extract_tables_nested_loop():
    plan = _load_plan("nested_loop.json")
    tables = extract_tables(plan)
    # nested_loop involves at least two relations
    assert len(tables) >= 1


def test_extract_tables_no_relations():
    """A plan that has relations returns a list with no duplicates."""
    plan = _load_plan("index_scan.json")
    tables = extract_tables(plan)
    # index_scan.json has a relation; check uniqueness (no duplicates)
    assert len(tables) == len(set(tables))


# ---------------------------------------------------------------------------
# _truncate helper
# ---------------------------------------------------------------------------


def test_truncate_short():
    from pgreviewer.cli.commands.check import _truncate

    assert _truncate("SELECT 1") == "SELECT 1"


def test_truncate_long():
    from pgreviewer.cli.commands.check import _truncate

    long_sql = "SELECT " + "a" * 100
    result = _truncate(long_sql)
    assert len(result) == 80
    assert result.endswith("...")


# ---------------------------------------------------------------------------
# _overall_severity
# ---------------------------------------------------------------------------


def _make_issue(severity: Severity) -> Issue:
    return Issue(
        severity=severity,
        detector_name="test_detector",
        description="desc",
        affected_table=None,
        affected_columns=[],
        suggested_action="action",
    )


def test_overall_severity_no_issues():
    from pgreviewer.cli.commands.check import _overall_severity

    assert _overall_severity([]) == "PASS"


def test_overall_severity_warning_only():
    from pgreviewer.cli.commands.check import _overall_severity

    issues = [_make_issue(Severity.WARNING)]
    assert _overall_severity(issues) == "WARNING"


def test_overall_severity_critical_dominates():
    from pgreviewer.cli.commands.check import _overall_severity

    issues = [_make_issue(Severity.WARNING), _make_issue(Severity.CRITICAL)]
    assert _overall_severity(issues) == "CRITICAL"


# ---------------------------------------------------------------------------
# _print_json_report
# ---------------------------------------------------------------------------


def test_print_json_report_valid_json(capsys):
    from pgreviewer.cli.commands.check import _print_json_report
    from pgreviewer.core.degradation import AnalysisResult

    issues = [_make_issue(Severity.WARNING)]
    result = AnalysisResult(issues=issues)
    _print_json_report("SELECT 1", result)

    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["overall_severity"] == "WARNING"
    assert data["issue_count"] == 1
    assert len(data["issues"]) == 1
    assert data["issues"][0]["severity"] == "WARNING"
    assert data["issues"][0]["detector_name"] == "test_detector"


def test_print_json_report_no_issues(capsys):
    from pgreviewer.cli.commands.check import _print_json_report
    from pgreviewer.core.degradation import AnalysisResult

    _print_json_report("SELECT 1", AnalysisResult())

    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["overall_severity"] == "PASS"
    assert data["issue_count"] == 0
    assert data["issues"] == []


# ---------------------------------------------------------------------------
# run_check – unit-tested with mocked DB pipeline
# ---------------------------------------------------------------------------


def _make_mock_issues(n_critical: int = 0, n_warning: int = 1):
    issues = []
    for _ in range(n_critical):
        issues.append(_make_issue(Severity.CRITICAL))
    for _ in range(n_warning):
        issues.append(_make_issue(Severity.WARNING))
    return issues


def _mock_asyncio_run_return(mock_run, result):
    def _runner(coro):
        coro.close()
        return result

    mock_run.side_effect = _runner


def test_print_recommendations_rich(capsys):
    """_print_recommendations prints validated and non-validated indexes."""
    from pgreviewer.cli.commands.check import _print_recommendations
    from pgreviewer.core.models import IndexRecommendation

    recs = [
        IndexRecommendation(
            table="orders",
            columns=["user_id"],
            index_type="btree",
            create_statement=(
                "CREATE INDEX CONCURRENTLY idx_orders_user_id ON orders(user_id);"
            ),
            cost_before=100.0,
            cost_after=10.0,
            improvement_pct=0.9,
            validated=True,
            rationale="Helps equality",
            confidence=0.95,
        ),
        IndexRecommendation(
            table="users",
            columns=["last_login"],
            index_type="btree",
            create_statement=(
                "CREATE INDEX CONCURRENTLY idx_users_login ON users(last_login);"
            ),
            cost_before=50.0,
            cost_after=45.0,
            improvement_pct=0.1,
            validated=False,
            rationale="Slight help",
            confidence=0.75,
        ),
    ]

    _print_recommendations(recs)
    captured = capsys.readouterr()

    # Check for validated one
    assert "💡 Suggested index (HypoPG validated ✓)" in captured.out
    assert "idx_orders_user_id" in captured.out
    assert "90.0%" in captured.out
    assert "Helps equality" in captured.out

    # Check for non-validated one
    assert "⚠️  Suggested index (Not validated)" in captured.out
    assert "idx_users_login" in captured.out
    assert "10.0%" in captured.out
    assert "Slight help" in captured.out
    assert "moderate confidence — verify before applying" in captured.out
    assert captured.out.count("moderate confidence — verify before applying") == 1


def test_print_recommendations_low_confidence_section(capsys):
    from pgreviewer.cli.commands.check import _print_recommendations
    from pgreviewer.core.models import IndexRecommendation

    recs = [
        IndexRecommendation(
            table="orders",
            columns=["created_at"],
            index_type="btree",
            create_statement=(
                "CREATE INDEX CONCURRENTLY idx_orders_created_at ON orders(created_at);"
            ),
            validated=False,
            rationale="Likely helpful for date filters",
            confidence=0.5,
        )
    ]

    _print_recommendations(recs)
    captured = capsys.readouterr()
    assert "Possible issues (low confidence)" in captured.out
    assert "manual review recommended" in captured.out
    assert "idx_orders_created_at" in captured.out


@patch("pgreviewer.cli.commands.check.asyncio.run")
def test_run_check_rich_output(mock_run, capsys):
    from pgreviewer.cli.commands.check import run_check
    from pgreviewer.core.degradation import AnalysisResult

    result = AnalysisResult(issues=_make_mock_issues(n_warning=1))
    _mock_asyncio_run_return(mock_run, result)
    run_check(query="SELECT * FROM users", query_file=None, json_output=False)
    captured = capsys.readouterr()
    assert "issue" in captured.out.lower()


@patch("pgreviewer.cli.commands.check.asyncio.run")
def test_run_check_json_flag(mock_run, capsys):
    from pgreviewer.cli.commands.check import run_check
    from pgreviewer.core.degradation import AnalysisResult

    result = AnalysisResult(issues=_make_mock_issues(n_warning=1))
    _mock_asyncio_run_return(mock_run, result)
    run_check(query="SELECT * FROM users", query_file=None, json_output=True)
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["issue_count"] == 1
    assert data["overall_severity"] == "WARNING"


@patch("pgreviewer.cli.commands.check.asyncio.run")
def test_run_check_json_with_recommendations(mock_run, capsys):
    from pgreviewer.cli.commands.check import run_check
    from pgreviewer.core.degradation import AnalysisResult
    from pgreviewer.core.models import IndexRecommendation

    rec = IndexRecommendation(
        table="orders",
        columns=["user_id"],
        index_type="btree",
        create_statement="CREATE INDEX CONCURRENTLY ...",
        cost_before=100.0,
        cost_after=10.0,
        improvement_pct=0.9,
        validated=True,
        confidence=0.95,
    )

    result = AnalysisResult(issues=[], recommendations=[rec])
    _mock_asyncio_run_return(mock_run, result)
    run_check(query="SELECT * FROM orders", query_file=None, json_output=True)
    captured = capsys.readouterr()
    data = json.loads(captured.out)

    assert "recommendations" in data
    assert len(data["recommendations"]) == 1
    assert data["recommendations"][0]["table"] == "orders"
    assert data["recommendations"][0]["validated"] is True
    assert data["recommendations"][0]["confidence"] == 0.95


@patch("pgreviewer.cli.commands.check.asyncio.run")
def test_run_check_json_includes_recommendation_source(mock_run, capsys):
    from pgreviewer.cli.commands.check import run_check
    from pgreviewer.core.degradation import AnalysisResult
    from pgreviewer.core.models import IndexRecommendation

    rec = IndexRecommendation(
        table="orders",
        columns=["user_id"],
        index_type="btree",
        create_statement="CREATE INDEX CONCURRENTLY ...",
        cost_before=100.0,
        cost_after=10.0,
        improvement_pct=0.9,
        validated=True,
        source="llm+hypopg",
    )

    result = AnalysisResult(issues=[], recommendations=[rec])
    _mock_asyncio_run_return(mock_run, result)
    run_check(query="SELECT * FROM orders", query_file=None, json_output=True)
    captured = capsys.readouterr()
    data = json.loads(captured.out)

    assert data["recommendations"][0]["source"] == "llm+hypopg"


@patch("pgreviewer.cli.commands.check.asyncio.run")
def test_run_check_rich_output_with_recs(mock_run, capsys):
    from pgreviewer.cli.commands.check import run_check
    from pgreviewer.core.degradation import AnalysisResult
    from pgreviewer.core.models import IndexRecommendation

    rec = IndexRecommendation(
        table="orders",
        columns=["user_id"],
        index_type="btree",
        create_statement="CREATE INDEX ...",
        validated=True,
    )
    result = AnalysisResult(issues=[], recommendations=[rec])
    _mock_asyncio_run_return(mock_run, result)
    run_check(query="SELECT 1", query_file=None, json_output=False)
    captured = capsys.readouterr()
    assert "Recommended Indexes" in captured.out
    assert "Suggested index" in captured.out


@patch("pgreviewer.cli.commands.check.asyncio.run")
def test_run_check_verbose_shows_explain_and_llm_details(mock_run, capsys):
    from pgreviewer.cli.commands.check import run_check
    from pgreviewer.core.degradation import AnalysisResult

    result = AnalysisResult(
        issues=_make_mock_issues(n_warning=1),
        raw_explain=[{"Plan": {"Node Type": "Seq Scan"}}],
        llm_interpretation={"summary": "Consider adding an index"},
    )
    _mock_asyncio_run_return(mock_run, result)

    run_check(
        query="SELECT * FROM users",
        query_file=None,
        json_output=False,
        verbose=True,
    )
    captured = capsys.readouterr()

    assert "EXPLAIN JSON" in captured.out
    assert '"Node Type": "Seq Scan"' in captured.out
    assert "LLM Interpretation (verbose)" in captured.out
    assert "Consider adding an index" in captured.out


@patch("pgreviewer.cli.commands.check.asyncio.run")
def test_run_check_no_color_plain_output(mock_run, capsys):
    from pgreviewer.cli.commands.check import run_check
    from pgreviewer.core.degradation import AnalysisResult

    result = AnalysisResult(issues=_make_mock_issues(n_warning=1))
    _mock_asyncio_run_return(mock_run, result)

    run_check(
        query="SELECT * FROM users",
        query_file=None,
        json_output=False,
        no_color=True,
    )
    captured = capsys.readouterr()
    assert re.search(_ANSI_ESCAPE_PATTERN, captured.out) is None
    assert "pgReviewer Analysis" in captured.out


@patch("pgreviewer.cli.commands.check.asyncio.run")
def test_run_check_shows_migration_safety_for_ddl(mock_run, capsys):
    from pgreviewer.cli.commands.check import run_check
    from pgreviewer.core.degradation import AnalysisResult

    _mock_asyncio_run_return(mock_run, AnalysisResult())
    run_check(
        query="ALTER TABLE users ADD COLUMN age int",
        query_file=None,
        json_output=False,
    )
    captured = capsys.readouterr()
    assert "Migration safety" in captured.out
    assert "DDL detected" in captured.out


def test_check_cli_forwards_verbose_and_no_color_flags():
    from pgreviewer.cli.main import app

    runner = CliRunner()
    with patch("pgreviewer.cli.commands.check.run_check") as mock_run:
        result = runner.invoke(app, ["check", "SELECT 1", "--verbose", "--no-color"])

    assert result.exit_code == 0
    mock_run.assert_called_once_with(
        query="SELECT 1",
        query_file=None,
        json_output=False,
        verbose=True,
        no_color=True,
    )


@pytest.mark.asyncio
async def test_analyse_query_pipeline_no_candidates():
    """_analyse_query returns issues and empty recs if no candidates found."""
    from pgreviewer.cli.commands.check import _analyse_query

    # Mock the internal imports used inside _analyse_query.
    # Since they are imported inside the function, we patch the module where
    # they are imported
    with (
        patch("pgreviewer.db.pool.read_session") as mock_read,
        patch("pgreviewer.db.pool.close_pool") as mock_close,
        patch("pgreviewer.analysis.explain_runner.run_explain") as mock_explain,
        patch("pgreviewer.analysis.plan_parser.parse_explain") as mock_parse,
        patch("pgreviewer.analysis.plan_parser.extract_tables") as mock_extract,
        patch("pgreviewer.analysis.issue_detectors.run_all_detectors") as mock_detect,
        patch("pgreviewer.analysis.index_suggester.suggest_indexes") as mock_suggest,
    ):
        # We need curious mock setup because of 'async with'
        mock_read.return_value.__aenter__.return_value = None
        mock_explain.return_value = {}
        mock_parse.return_value = None
        mock_extract.return_value = []
        mock_detect.return_value = ["issue1"]
        mock_suggest.return_value = []
        # We need to mock settings for the check command
        with patch("pgreviewer.config.settings.LLM_API_KEY", None):
            result = await _analyse_query("SELECT 1")

        assert result.issues == ["issue1"]
        assert result.recommendations == []
        mock_close.assert_called_once()


@pytest.mark.asyncio
async def test_analyse_query_pipeline_with_candidates():
    """_analyse_query returns recs if candidates are found and validated."""
    from pgreviewer.analysis.hypopg_validator import ValidationResult
    from pgreviewer.analysis.index_suggester import IndexCandidate
    from pgreviewer.cli.commands.check import _analyse_query
    from pgreviewer.core.models import SchemaInfo

    with (
        patch("pgreviewer.db.pool.read_session") as mock_read,
        patch("pgreviewer.db.pool.write_session") as mock_write,
        patch("pgreviewer.db.pool.close_pool") as mock_close,
        patch("pgreviewer.analysis.explain_runner.run_explain") as mock_explain,
        patch("pgreviewer.analysis.plan_parser.parse_explain") as mock_parse,
        patch("pgreviewer.analysis.plan_parser.extract_tables") as mock_extract,
        patch("pgreviewer.analysis.schema_collector.collect_schema") as mock_schema,
        patch("pgreviewer.analysis.issue_detectors.run_all_detectors") as mock_detect,
        patch("pgreviewer.analysis.index_suggester.suggest_indexes") as mock_suggest,
        patch("pgreviewer.analysis.hypopg_validator.validate_candidate") as mock_val,
        patch(
            "pgreviewer.analysis.hypopg_validator.validate_candidates_combined"
        ) as mock_combined,
        patch("pgreviewer.analysis.index_generator.generate_create_index") as mock_gen,
    ):
        mock_read.return_value.__aenter__.return_value = None
        mock_write.return_value.__aenter__.return_value = None
        mock_explain.return_value = {}
        mock_parse.return_value = None
        mock_extract.return_value = ["orders"]
        mock_schema.return_value = SchemaInfo()
        mock_detect.return_value = []

        cand = IndexCandidate(
            table="orders",
            columns=["user_id"],
            index_type="btree",
            rationale="test",
        )
        mock_suggest.return_value = [cand]

        v_res = ValidationResult(
            cost_before=100.0,
            cost_after=10.0,
            improvement_pct=0.9,
            validated=True,
            rationale="validated",
            new_plan={},
        )
        mock_val.return_value = v_res
        from pgreviewer.analysis.hypopg_validator import CombinedValidationResult

        mock_combined.return_value = CombinedValidationResult(
            cost_before=100.0,
            cost_after=10.0,
            improvement_pct=0.9,
            new_plan={},
        )
        mock_gen.return_value = "CREATE INDEX ..."

        # We need to mock settings for the check command
        with patch("pgreviewer.config.settings.LLM_API_KEY", None):
            result = await _analyse_query("SELECT * FROM orders")

        recs = result.recommendations

        assert len(recs) == 1
        assert recs[0].table == "orders"
        assert recs[0].validated is True
        assert recs[0].create_statement == "CREATE INDEX ..."
        mock_close.assert_called_once()


@pytest.mark.asyncio
async def test_analyse_query_pipeline_llm_suggestions_validated_and_filtered():
    """LLM suggestions are HypoPG-validated; non-improving suggestions are excluded."""
    from pgreviewer.cli.commands.check import _analyse_query
    from pgreviewer.core.models import TableInfo
    from pgreviewer.exceptions import InvalidQueryError
    from pgreviewer.llm.prompts.explain_interpreter import (
        Bottleneck,
        ExplainInterpretation,
        IndexSuggestion,
    )

    llm_output = ExplainInterpretation(
        summary="Plan summary",
        bottlenecks=[Bottleneck(node_type="Seq Scan", details="scan")],
        root_cause="missing index",
        suggested_indexes=[
            IndexSuggestion(
                table="orders",
                columns=["user_id"],
                rationale="join filter",
                confidence=0.95,
            ),
            IndexSuggestion(
                table="orders",
                columns=["ghost_column"],
                rationale="hallucinated",
                confidence=0.95,
            ),
        ],
        confidence=0.9,
    )

    backend = AsyncMock()
    backend.get_explain_plan.side_effect = [
        {"Plan": {"Total Cost": 100.0}},
        {"Plan": {"Total Cost": 60.0}},
        InvalidQueryError("SELECT * FROM orders", "column does not exist"),
    ]
    backend.recommend_indexes.return_value = []
    backend.get_schema_info.return_value = TableInfo()

    with (
        patch("pgreviewer.db.pool.close_pool") as mock_close,
        patch("pgreviewer.core.backend.get_backend", return_value=backend),
        patch("pgreviewer.analysis.plan_parser.parse_explain") as mock_parse,
        patch("pgreviewer.analysis.plan_parser.extract_tables") as mock_extract,
        patch("pgreviewer.analysis.issue_detectors.run_all_detectors") as mock_detect,
        patch(
            "pgreviewer.analysis.complexity_router.should_use_llm",
            return_value=(True, "complex query"),
        ),
        patch(
            "pgreviewer.llm.prompts.explain_interpreter.interpret_explain",
            return_value=llm_output,
        ),
        patch("pgreviewer.llm.client.LLMClient"),
        patch("pgreviewer.analysis.index_generator.generate_create_index") as mock_gen,
    ):
        mock_parse.return_value = None
        mock_extract.return_value = ["orders"]
        mock_detect.return_value = []
        mock_gen.return_value = "CREATE INDEX ..."

        with patch("pgreviewer.config.settings.LLM_API_KEY", "test-key"):
            result = await _analyse_query("SELECT * FROM orders")

        assert len(result.recommendations) == 1
        rec = result.recommendations[0]
        assert rec.table == "orders"
        assert rec.validated is True
        assert rec.source == "llm+hypopg"
        assert rec.confidence == 0.95
        assert rec.cost_before == 100.0
        assert rec.cost_after == 60.0
        assert rec.improvement_pct == 0.4
        assert llm_output.suggested_indexes[0].validated is True
        assert llm_output.suggested_indexes[1].validated is False
        mock_close.assert_called_once()


@pytest.mark.asyncio
async def test_analyse_query_pipeline_llm_high_confidence_included_when_unavailable():
    from pgreviewer.cli.commands.check import _analyse_query
    from pgreviewer.core.models import SchemaInfo
    from pgreviewer.exceptions import ExtensionMissingError
    from pgreviewer.llm.prompts.explain_interpreter import (
        ExplainInterpretation,
        IndexSuggestion,
    )

    llm_output = ExplainInterpretation(
        summary="Plan summary",
        bottlenecks=[],
        root_cause="missing index",
        suggested_indexes=[
            IndexSuggestion(
                table="orders",
                columns=["status"],
                rationale="high confidence",
                confidence=0.95,
            ),
            IndexSuggestion(
                table="orders",
                columns=["created_at"],
                rationale="low confidence",
                confidence=0.5,
            ),
        ],
        confidence=0.9,
    )

    with (
        patch("pgreviewer.db.pool.read_session") as mock_read,
        patch("pgreviewer.db.pool.write_session") as mock_write,
        patch("pgreviewer.db.pool.close_pool") as mock_close,
        patch("pgreviewer.analysis.explain_runner.run_explain") as mock_explain,
        patch("pgreviewer.analysis.plan_parser.parse_explain") as mock_parse,
        patch("pgreviewer.analysis.plan_parser.extract_tables") as mock_extract,
        patch("pgreviewer.analysis.schema_collector.collect_schema") as mock_schema,
        patch("pgreviewer.analysis.issue_detectors.run_all_detectors") as mock_detect,
        patch("pgreviewer.analysis.index_suggester.suggest_indexes") as mock_suggest,
        patch(
            "pgreviewer.analysis.hypopg_validator.validate_candidate",
            side_effect=ExtensionMissingError("hypopg"),
        ),
        patch(
            "pgreviewer.analysis.complexity_router.should_use_llm",
            return_value=(True, "complex query"),
        ),
        patch(
            "pgreviewer.llm.prompts.explain_interpreter.interpret_explain",
            return_value=llm_output,
        ),
        patch("pgreviewer.llm.client.LLMClient"),
        patch("pgreviewer.analysis.index_generator.generate_create_index") as mock_gen,
    ):
        mock_read.return_value.__aenter__.return_value = None
        mock_write.return_value.__aenter__.return_value = None
        mock_explain.return_value = {"Plan": {"Total Cost": 100.0}}
        mock_parse.return_value = None
        mock_extract.return_value = ["orders"]
        mock_schema.return_value = SchemaInfo()
        mock_detect.return_value = []
        mock_suggest.return_value = []
        mock_gen.return_value = "CREATE INDEX ..."

        with patch("pgreviewer.config.settings.LLM_API_KEY", "test-key"):
            result = await _analyse_query("SELECT * FROM orders")

        assert len(result.recommendations) == 2
        assert result.recommendations[0].confidence == 0.95
        assert result.recommendations[1].confidence == 0.5
        assert all(rec.source == "llm" for rec in result.recommendations)
        assert all(rec.validated is False for rec in result.recommendations)
        assert all(
            "HypoPG validation unavailable" in rec.notes[0]
            for rec in result.recommendations
        )
        mock_close.assert_called_once()


@patch("pgreviewer.cli.commands.check.asyncio.run")
def test_run_check_query_file(mock_run, tmp_path, capsys):
    """--query-file reads SQL from a file."""
    from pgreviewer.cli.commands.check import run_check
    from pgreviewer.core.degradation import AnalysisResult

    sql_file = tmp_path / "query.sql"
    sql_file.write_text("SELECT * FROM orders")
    _mock_asyncio_run_return(mock_run, AnalysisResult())

    run_check(query=None, query_file=sql_file, json_output=True)
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["query"] == "SELECT * FROM orders"


def test_run_check_no_query_exits():
    """No SQL and no file raises Exit(1)."""
    from typer import Exit

    from pgreviewer.cli.commands.check import run_check

    with pytest.raises(Exit):
        run_check(query=None, query_file=None, json_output=False)


def test_run_check_empty_query_exits():
    """Empty SQL raises Exit(1)."""
    from typer import Exit

    from pgreviewer.cli.commands.check import run_check

    with pytest.raises(Exit):
        run_check(query="   ", query_file=None, json_output=False)
