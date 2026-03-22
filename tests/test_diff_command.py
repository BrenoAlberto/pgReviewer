"""Tests for pgr diff --git-ref / --staged functionality."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from pgreviewer.analysis.cross_correlator import CrossCuttingFinding
from pgreviewer.cli.commands.diff import (
    _analyze_all_queries,
    _apply_workload_correlation,
    _get_git_diff,
    _print_json_diff_report,
)
from pgreviewer.cli.main import app
from pgreviewer.core.degradation import AnalysisResult
from pgreviewer.core.models import (
    ExtractedQuery,
    Issue,
    SchemaInfo,
    Severity,
    SlowQuery,
    TableInfo,
)
from pgreviewer.parsing.diff_parser import ChangedFile
from pgreviewer.parsing.model_differ import ModelDiff
from pgreviewer.parsing.sqlalchemy_analyzer import IndexDef

# ---------------------------------------------------------------------------
# _get_git_diff – happy paths
# ---------------------------------------------------------------------------


def _make_proc(stdout: str = "", returncode: int = 0, stderr: str = "") -> MagicMock:
    proc = MagicMock()
    proc.stdout = stdout
    proc.returncode = returncode
    proc.stderr = stderr
    return proc


def test_get_git_diff_with_ref():
    """_get_git_diff runs 'git diff <ref>'."""
    expected = "diff --git a/foo.sql b/foo.sql\n"
    with patch("subprocess.run", return_value=_make_proc(stdout=expected)) as mock_run:
        result = _get_git_diff(git_ref="HEAD~1")

    mock_run.assert_called_once_with(
        ["git", "diff", "HEAD~1"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result == expected


def test_get_git_diff_staged():
    """_get_git_diff runs 'git diff --staged' when staged=True."""
    expected = "diff --git a/bar.sql b/bar.sql\n"
    with patch("subprocess.run", return_value=_make_proc(stdout=expected)) as mock_run:
        result = _get_git_diff(staged=True)

    mock_run.assert_called_once_with(
        ["git", "diff", "--staged"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result == expected


def test_get_git_diff_empty_output():
    """An empty diff (no changes) should return an empty string without error."""
    with patch("subprocess.run", return_value=_make_proc(stdout="")):
        result = _get_git_diff(staged=True)
    assert result == ""


# ---------------------------------------------------------------------------
# _get_git_diff – error handling
# ---------------------------------------------------------------------------


def test_get_git_diff_git_not_installed():
    """FileNotFoundError from subprocess → clear ValueError."""
    with (
        patch("subprocess.run", side_effect=FileNotFoundError),
        pytest.raises(ValueError, match="git is not installed"),
    ):
        _get_git_diff(git_ref="HEAD~1")


def test_get_git_diff_generic_error_returncode_127():
    """returncode=127 (command not found shell error) → generic ValueError."""
    proc = _make_proc(returncode=127, stderr="git: command not found")
    with (
        patch("subprocess.run", return_value=proc),
        pytest.raises(ValueError, match="git diff failed"),
    ):
        _get_git_diff(staged=True)


def test_get_git_diff_no_ref_no_staged_raises():
    """Calling _get_git_diff with no ref and staged=False raises ValueError."""
    with pytest.raises(ValueError, match="Either git_ref or staged"):
        _get_git_diff()


def test_get_git_diff_not_a_git_repo():
    """'not a git repository' in stderr → clear ValueError."""
    proc = _make_proc(
        returncode=128,
        stderr="fatal: not a git repository (or any of the parent directories): .git",
    )
    with (
        patch("subprocess.run", return_value=proc),
        pytest.raises(ValueError, match="Not inside a git repository"),
    ):
        _get_git_diff(git_ref="HEAD~1")


def test_get_git_diff_bad_revision():
    """Unknown ref → ValueError mentioning the bad ref."""
    proc = _make_proc(
        returncode=128,
        stderr="fatal: ambiguous argument 'HEAD~999': unknown revision",
    )
    with (
        patch("subprocess.run", return_value=proc),
        pytest.raises(ValueError, match="Invalid git ref 'HEAD~999'"),
    ):
        _get_git_diff(git_ref="HEAD~999")


def test_get_git_diff_generic_error():
    """Non-zero return with unknown stderr → generic ValueError."""
    proc = _make_proc(returncode=1, stderr="some unexpected git error")
    with (
        patch("subprocess.run", return_value=proc),
        pytest.raises(ValueError, match="git diff failed"),
    ):
        _get_git_diff(staged=True)


# ---------------------------------------------------------------------------
# run_diff – input-source validation
# ---------------------------------------------------------------------------


def _run_diff_expect_exit(**kwargs) -> int:
    """Call run_diff and return the exit code from typer.Exit / SystemExit."""
    import click

    from pgreviewer.cli.commands.diff import run_diff

    defaults: dict = {
        "diff_file": None,
        "git_ref": None,
        "staged": False,
        "json_output": False,
        "only_critical": False,
        "ci": False,
        "severity_threshold": "critical",
    }
    defaults.update(kwargs)
    try:
        run_diff(**defaults)  # type: ignore[arg-type]
    except SystemExit as e:
        return int(e.code)
    except click.exceptions.Exit as e:
        return int(e.exit_code)
    return 0


def test_run_diff_no_source_exits_with_error():
    """run_diff with no source must exit with code 1."""
    code = _run_diff_expect_exit()
    assert code == 1


def test_run_diff_multiple_sources_exits_with_error(tmp_path):
    """run_diff with more than one source must exit with code 1."""
    dummy_file = tmp_path / "test.patch"
    dummy_file.write_text("")
    code = _run_diff_expect_exit(diff_file=dummy_file, git_ref="HEAD~1")
    assert code == 1


def test_run_diff_staged_and_git_ref_exits_with_error():
    """run_diff with both --staged and --git-ref must exit with code 1."""
    code = _run_diff_expect_exit(git_ref="main", staged=True)
    assert code == 1


# ---------------------------------------------------------------------------
# run_diff – git error propagation
# ---------------------------------------------------------------------------


def test_run_diff_git_error_exits_with_code_1():
    """When _get_git_diff raises ValueError run_diff must exit 1."""
    with patch(
        "pgreviewer.cli.commands.diff._get_git_diff",
        side_effect=ValueError("Not inside a git repository"),
    ):
        code = _run_diff_expect_exit(git_ref="HEAD~1")
    assert code == 1


def _analysis_item(severity: Severity) -> dict:
    issue = Issue(
        severity=severity,
        detector_name="detector",
        description="desc",
        affected_table=None,
        affected_columns=[],
        suggested_action="action",
    )
    query = ExtractedQuery(
        sql="SELECT 1",
        source_file="foo.sql",
        line_number=1,
        extraction_method="raw",
        confidence=1.0,
    )
    result = AnalysisResult(issues=[issue], recommendations=[])
    return {
        "query_obj": query,
        "analysis_result": result,
        "issues": [issue],
        "recs": [],
    }


def test_run_diff_ci_critical_threshold_passes_with_only_warnings(
    monkeypatch, tmp_path, capsys
):
    from pgreviewer.cli.commands.diff import run_diff

    monkeypatch.chdir(tmp_path)
    (tmp_path / "foo.sql").write_text("SELECT 1;\n", encoding="utf-8")

    with (
        patch("pgreviewer.cli.commands.diff._get_git_diff", return_value="dummy"),
        patch(
            "pgreviewer.parsing.diff_parser.parse_diff",
            return_value=[ChangedFile(path="foo.sql")],
        ),
        patch(
            "pgreviewer.parsing.extraction_router.route_extraction",
            return_value=[
                ExtractedQuery(
                    sql="SELECT 1;",
                    source_file="foo.sql",
                    line_number=1,
                    extraction_method="raw",
                    confidence=1.0,
                )
            ],
        ),
        patch(
            "pgreviewer.cli.commands.diff._analyze_all_queries",
            return_value=[_analysis_item(Severity.WARNING)],
        ),
        patch(
            "pgreviewer.analysis.cross_correlator.correlate_findings",
            return_value=[],
        ),
        patch("pgreviewer.cli.commands.diff._print_rich_diff_report"),
    ):
        run_diff(
            diff_file=None,
            git_ref="HEAD~1",
            staged=False,
            json_output=False,
            only_critical=False,
            ci=True,
            severity_threshold="critical",
        )
    output = capsys.readouterr().out
    assert (
        "Severity threshold: critical. Found: 0 critical, 1 warning, 0 info. "
        "Result: PASS" in output
    )


def test_run_diff_ci_critical_threshold_fails_with_critical(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "foo.sql").write_text("SELECT 1;\n", encoding="utf-8")
    with (
        patch("pgreviewer.cli.commands.diff._get_git_diff", return_value="dummy"),
        patch(
            "pgreviewer.parsing.diff_parser.parse_diff",
            return_value=[ChangedFile(path="foo.sql")],
        ),
        patch(
            "pgreviewer.parsing.extraction_router.route_extraction",
            return_value=[
                ExtractedQuery(
                    sql="SELECT 1;",
                    source_file="foo.sql",
                    line_number=1,
                    extraction_method="raw",
                    confidence=1.0,
                )
            ],
        ),
        patch(
            "pgreviewer.cli.commands.diff._analyze_all_queries",
            return_value=[_analysis_item(Severity.CRITICAL)],
        ),
        patch(
            "pgreviewer.analysis.cross_correlator.correlate_findings",
            return_value=[],
        ),
        patch("pgreviewer.cli.commands.diff._print_rich_diff_report"),
    ):
        code = _run_diff_expect_exit(
            git_ref="HEAD~1", ci=True, severity_threshold="critical"
        )
    assert code == 1


def test_run_diff_ci_none_threshold_always_passes(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "foo.sql").write_text("SELECT 1;\n", encoding="utf-8")

    with (
        patch("pgreviewer.cli.commands.diff._get_git_diff", return_value="dummy"),
        patch(
            "pgreviewer.parsing.diff_parser.parse_diff",
            return_value=[ChangedFile(path="foo.sql")],
        ),
        patch(
            "pgreviewer.parsing.extraction_router.route_extraction",
            return_value=[
                ExtractedQuery(
                    sql="SELECT 1;",
                    source_file="foo.sql",
                    line_number=1,
                    extraction_method="raw",
                    confidence=1.0,
                )
            ],
        ),
        patch(
            "pgreviewer.cli.commands.diff._analyze_all_queries",
            return_value=[_analysis_item(Severity.CRITICAL)],
        ),
        patch(
            "pgreviewer.analysis.cross_correlator.correlate_findings",
            return_value=[],
        ),
        patch("pgreviewer.cli.commands.diff._print_rich_diff_report"),
    ):
        code = _run_diff_expect_exit(
            git_ref="HEAD~1", ci=True, severity_threshold="none"
        )
    assert code == 0


# ---------------------------------------------------------------------------
# run_diff – empty diff from git (no SQL changes)
# ---------------------------------------------------------------------------


def test_run_diff_empty_git_diff_no_crash():
    """An empty git diff should not raise an exception."""
    import click

    from pgreviewer.cli.commands.diff import run_diff

    with patch(
        "pgreviewer.cli.commands.diff._get_git_diff",
        return_value="",
    ):
        try:
            run_diff(
                diff_file=None,
                git_ref="HEAD~1",
                staged=False,
                json_output=False,
                only_critical=False,
            )
        except SystemExit as e:
            pytest.fail(f"run_diff raised SystemExit {e.code} on empty diff")
        except click.exceptions.Exit as e:
            pytest.fail(f"run_diff raised typer.Exit {e.exit_code} on empty diff")


def test_run_diff_fast_precheck_skips_non_sql_paths(monkeypatch):
    from pgreviewer.cli.commands.diff import run_diff
    from pgreviewer.config import settings

    monkeypatch.setattr(settings, "TRIGGER_PATHS", [])
    with (
        patch("pgreviewer.cli.commands.diff._get_git_diff", return_value="dummy"),
        patch(
            "pgreviewer.parsing.diff_parser.parse_diff",
            return_value=[ChangedFile(path="README.md")],
        ),
        patch("pgreviewer.cli.commands.diff._analyze_all_queries") as mock_analyze,
    ):
        run_diff(
            diff_file=None,
            git_ref="HEAD~1",
            staged=False,
            json_output=False,
            only_critical=False,
        )

    mock_analyze.assert_not_called()


def test_run_diff_honors_custom_trigger_paths(monkeypatch, tmp_path):
    from pgreviewer.cli.commands.diff import run_diff
    from pgreviewer.config import settings
    from pgreviewer.core.models import ExtractedQuery

    monkeypatch.setattr(settings, "TRIGGER_PATHS", ["custom/sql/**"])
    monkeypatch.chdir(tmp_path)
    patch_text = (
        "diff --git a/custom/sql/report.sql b/custom/sql/report.sql\n"
        "--- a/custom/sql/report.sql\n"
        "+++ b/custom/sql/report.sql\n"
        "@@ -0,0 +1 @@\n"
        "+SELECT 1;\n"
    )
    query = ExtractedQuery(
        sql="SELECT 1;",
        source_file="custom/sql/report.sql",
        line_number=1,
        extraction_method="raw_sql",
        confidence=1.0,
    )

    with (
        patch("pgreviewer.cli.commands.diff._get_git_diff", return_value=patch_text),
        patch(
            "pgreviewer.parsing.extraction_router.route_extraction",
            return_value=[query],
        ),
        patch(
            "pgreviewer.cli.commands.diff._analyze_all_queries",
            return_value=[],
        ) as mock_analyze,
    ):
        Path("custom/sql").mkdir(parents=True, exist_ok=True)
        Path("custom/sql/report.sql").write_text("SELECT 1;\n", encoding="utf-8")
        run_diff(
            diff_file=None,
            git_ref="HEAD~1",
            staged=False,
            json_output=True,
            only_critical=False,
        )
    mock_analyze.assert_called_once()


@pytest.mark.asyncio
async def test_analyze_all_queries_includes_referenced_drop_column_issue():
    drop_query = ExtractedQuery(
        sql="ALTER TABLE users DROP COLUMN email;",
        source_file="migrations/0002_drop_email.sql",
        line_number=7,
        extraction_method="migration_sql",
        confidence=1.0,
    )
    app_query = ExtractedQuery(
        sql="SELECT email FROM users WHERE email IS NOT NULL;",
        source_file="app/users_repo.py",
        line_number=31,
        extraction_method="ast",
        confidence=0.9,
    )

    with patch(
        "pgreviewer.cli.commands.check._analyse_query",
        return_value=AnalysisResult(issues=[], recommendations=[]),
    ):
        results = await _analyze_all_queries(
            [drop_query, app_query], only_critical=False
        )

    assert len(results) == 2
    assert len(results[0]["issues"]) == 1
    assert results[0]["issues"][0].detector_name == "drop_column_still_referenced"


@pytest.mark.asyncio
async def test_analyze_all_queries_flags_drop_index_with_matching_workload():
    drop_index_query = ExtractedQuery(
        sql="DROP INDEX idx_orders_created_at;",
        source_file="migrations/0012_drop_idx.sql",
        line_number=3,
        extraction_method="migration_sql",
        confidence=1.0,
    )
    slow_queries = [
        SlowQuery(
            query_text="SELECT * FROM orders WHERE created_at > $1;",
            calls=1_200,
            mean_exec_time_ms=45.0,
            total_exec_time_ms=54_000.0,
            rows=24_000,
        )
    ]

    class _FakeBackend:
        async def get_slow_queries(self, limit: int = 20) -> list[SlowQuery]:
            del limit
            return slow_queries

    with (
        patch(
            "pgreviewer.cli.commands.check._analyse_query",
            return_value=AnalysisResult(issues=[], recommendations=[]),
        ),
        patch("pgreviewer.core.backend.get_backend", return_value=_FakeBackend()),
    ):
        results = await _analyze_all_queries([drop_index_query], only_critical=False)

    issues = results[0]["issues"]
    detector_issues = [i for i in issues if i.detector_name == "drop_index_workload"]
    assert len(detector_issues) == 1
    issue = detector_issues[0]
    assert issue.severity == Severity.CRITICAL
    assert (
        "WARNING: dropping idx_orders_created_at — 1 queries in pg_stat_statements use "
        "this column (1200/day, avg 45.0ms)" in issue.description
    )


def test_print_json_diff_report_includes_cross_cutting_findings(capsys):
    finding = CrossCuttingFinding(
        issue=Issue(
            severity=Severity.CRITICAL,
            detector_name="cross_cutting_add_column_query_without_index",
            description="Correlated issue",
            affected_table="orders",
            affected_columns=["status"],
            suggested_action="Create index",
        ),
        migration_file="migrations/001_add_status.sql",
        migration_line=3,
        query_file="app/orders_repo.py",
        query_line=12,
    )

    _print_json_diff_report([], [], [], [finding])
    output = capsys.readouterr().out

    assert "cross_cutting_findings" in output
    assert "migrations/001_add_status.sql" in output
    assert "app/orders_repo.py" in output


def test_print_json_diff_report_includes_code_pattern_issues(capsys):
    issue = Issue(
        severity=Severity.WARNING,
        detector_name="n_plus_one_query",
        description="Potential N+1 query pattern",
        affected_table=None,
        affected_columns=[],
        suggested_action="Prefetch relations",
    )

    _print_json_diff_report([], [], [], [], [issue])
    output = capsys.readouterr().out

    assert "code_pattern_issues" in output
    assert "n_plus_one_query" in output


def test_print_json_diff_report_includes_loop_impact_estimate(capsys):
    issue = Issue(
        severity=Severity.CRITICAL,
        detector_name="query_in_loop",
        description="Potential N+1 query pattern",
        affected_table=None,
        affected_columns=[],
        suggested_action="Prefetch relations",
        context={"iterable_source_table": "orders"},
    )
    schema = SchemaInfo(tables={"orders": TableInfo(row_estimate=250_000)})

    _print_json_diff_report([], [], [], [], [issue], schema=schema)
    output = capsys.readouterr().out

    assert "impact_estimate" in output
    assert '"max_iterations": 250000' in output
    assert "~250 seconds of DB time" in output


def test_pgr_diff_dangerous_fixture_exits_non_zero(monkeypatch):
    async def _fake_analyse_query(_sql: str):
        return AnalysisResult(issues=[], recommendations=[])

    monkeypatch.setattr(
        "pgreviewer.cli.commands.check._analyse_query",
        _fake_analyse_query,
    )

    runner = CliRunner()
    result = runner.invoke(
        app, ["diff", "tests/fixtures/diffs/dangerous_migration.patch"]
    )

    assert result.exit_code != 0


def test_apply_workload_correlation_escalates_and_enriches_issues() -> None:
    query = ExtractedQuery(
        sql="SELECT * FROM users WHERE id = 42;",
        source_file="app/users_repo.py",
        line_number=10,
        extraction_method="ast",
        confidence=1.0,
    )
    info_issue = Issue(
        severity=Severity.INFO,
        detector_name="high_cost",
        description="Potentially expensive query",
        affected_table="users",
        affected_columns=["id"],
        suggested_action="Add index",
    )
    warning_issue = Issue(
        severity=Severity.WARNING,
        detector_name="sequential_scan",
        description="Seq scan observed",
        affected_table="users",
        affected_columns=["id"],
        suggested_action="Review plan",
    )
    results = [
        {
            "query_obj": query,
            "analysis_result": AnalysisResult(issues=[info_issue, warning_issue]),
            "issues": [info_issue, warning_issue],
            "recs": [],
        }
    ]
    slow_queries = [
        SlowQuery(
            query_text="SELECT * FROM users WHERE id = 99",
            calls=5_000,
            mean_exec_time_ms=1_250,
            total_exec_time_ms=1_500_000,
            rows=5_000,
        )
    ]

    _apply_workload_correlation(results, slow_queries)

    for issue in results[0]["issues"]:
        assert issue.severity == Severity.CRITICAL
        assert issue.context["workload_stats"] == {
            "calls_per_day": 5_000,
            "avg_time_ms": 1_250,
            "total_time_min_per_day": 25.0,
        }


def test_apply_removed_index_workload_correlation_adds_model_issue() -> None:
    from pgreviewer.cli.commands.diff import _apply_removed_index_workload_correlation

    model_diff = ModelDiff(
        class_name="Order",
        table_name="orders",
        removed_indexes=[
            IndexDef(name="idx_orders_created_at", columns=["created_at"])
        ],
    )
    model_diff_results = [
        {"file": "models.py", "diffs": [model_diff], "model_issues": []}
    ]
    slow_queries = [
        SlowQuery(
            query_text="SELECT id FROM orders WHERE created_at > $1",
            calls=200,
            mean_exec_time_ms=12.0,
            total_exec_time_ms=2_400.0,
            rows=400,
        )
    ]

    _apply_removed_index_workload_correlation(model_diff_results, slow_queries)

    detector_issues = [
        issue
        for issue in model_diff_results[0]["model_issues"]
        if issue.detector_name == "drop_index_workload"
    ]
    assert len(detector_issues) == 1


# ---------------------------------------------------------------------------
# --schema flag
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_analyze_all_queries_loads_explicit_schema_path(tmp_path):
    """When schema_path is provided, _analyze_all_queries loads it."""
    schema_sql = tmp_path / "schema.sql"
    schema_sql.write_text(
        '-- pgreviewer:stats {"orders":{"row_estimate":50000,"size_bytes":4096000,'
        '"indexes":[],"columns":[]}}\n'
    )

    query = ExtractedQuery(
        sql="CREATE INDEX ix_orders_user ON orders (user_id);",
        source_file="migrations/0001.sql",
        line_number=1,
        extraction_method="migration_sql",
        confidence=1.0,
    )

    results = await _analyze_all_queries(
        [query], only_critical=False, schema_path=schema_sql
    )

    # Schema was loaded — orders table should be present in schema seen by mutator.
    # The query itself is DDL so it returns migration issues only (no EXPLAIN).
    assert len(results) == 1


@pytest.mark.asyncio
async def test_analyze_all_queries_autodetects_schema_from_cwd(tmp_path, monkeypatch):
    """When .pgreviewer/schema.sql exists in CWD it is loaded automatically."""
    monkeypatch.chdir(tmp_path)
    pgr_dir = tmp_path / ".pgreviewer"
    pgr_dir.mkdir()
    (pgr_dir / "schema.sql").write_text(
        '-- pgreviewer:stats {"users":{"row_estimate":1000,"size_bytes":8192,'
        '"indexes":[],"columns":[]}}\n'
    )

    query = ExtractedQuery(
        sql="CREATE INDEX ix_users_email ON users (email);",
        source_file="migrations/0001.sql",
        line_number=1,
        extraction_method="migration_sql",
        confidence=1.0,
    )

    # No schema_path passed — should auto-detect from CWD
    results = await _analyze_all_queries([query], only_critical=False)
    assert len(results) == 1


@pytest.mark.asyncio
async def test_analyze_all_queries_no_schema_file_uses_empty_schema(
    tmp_path, monkeypatch
):
    """When no schema file exists, analysis proceeds with empty SchemaInfo."""
    monkeypatch.chdir(tmp_path)  # CWD has no .pgreviewer/schema.sql

    query = ExtractedQuery(
        sql="CREATE INDEX ix_orders_user ON orders (user_id);",
        source_file="migrations/0001.sql",
        line_number=1,
        extraction_method="migration_sql",
        confidence=1.0,
    )

    # Should not raise
    results = await _analyze_all_queries([query], only_critical=False)
    assert len(results) == 1


def test_diff_cli_accepts_schema_flag(tmp_path):
    """pgr diff accepts --schema flag without error."""
    schema_file = tmp_path / "schema.sql"
    schema_file.write_text(
        '-- pgreviewer:stats {"t":{"row_estimate":0,"size_bytes":0,'
        '"indexes":[],"columns":[]}}\n'
    )
    diff_file = tmp_path / "changes.patch"
    diff_file.write_text("")

    runner = CliRunner()
    with patch("pgreviewer.cli.commands.diff.run_diff") as mock_run:
        runner.invoke(
            app,
            ["diff", str(diff_file), "--schema", str(schema_file)],
        )
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args.kwargs
        assert call_kwargs["schema"] == schema_file
