"""Tests for pgr diff --git-ref / --staged functionality."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from pgreviewer.analysis.cross_correlator import CrossCuttingFinding
from pgreviewer.cli.commands.diff import (
    _analyze_all_queries,
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
    TableInfo,
)

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
