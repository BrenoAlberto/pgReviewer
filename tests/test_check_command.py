"""Tests for pgreviewer/cli/commands/check.py and plan_parser.extract_tables."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pgreviewer.analysis.plan_parser import extract_tables, parse_explain
from pgreviewer.core.models import Issue, Severity

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "explain"


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

    issues = [_make_issue(Severity.WARNING)]
    _print_json_report("SELECT 1", issues)

    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["overall_severity"] == "WARNING"
    assert data["issue_count"] == 1
    assert len(data["issues"]) == 1
    assert data["issues"][0]["severity"] == "WARNING"
    assert data["issues"][0]["detector_name"] == "test_detector"


def test_print_json_report_no_issues(capsys):
    from pgreviewer.cli.commands.check import _print_json_report

    _print_json_report("SELECT 1", [])

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


@patch("pgreviewer.cli.commands.check.asyncio.run")
def test_run_check_rich_output(mock_run, capsys):
    """run_check with rich output prints overall severity."""
    from pgreviewer.cli.commands.check import run_check

    mock_run.return_value = _make_mock_issues(n_warning=1)
    run_check(query="SELECT * FROM users", query_file=None, json_output=False)
    captured = capsys.readouterr()
    assert "issue" in captured.out.lower()


@patch("pgreviewer.cli.commands.check.asyncio.run")
def test_run_check_json_flag(mock_run, capsys):
    """--json flag produces valid JSON with issues."""
    from pgreviewer.cli.commands.check import run_check

    mock_run.return_value = _make_mock_issues(n_warning=1)
    run_check(query="SELECT * FROM users", query_file=None, json_output=True)
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["issue_count"] == 1
    assert data["overall_severity"] == "WARNING"


@patch("pgreviewer.cli.commands.check.asyncio.run")
def test_run_check_pass_no_issues(mock_run, capsys):
    """SELECT 1 style query with no issues shows PASS."""
    from pgreviewer.cli.commands.check import run_check

    mock_run.return_value = []
    run_check(query="SELECT 1", query_file=None, json_output=False)
    captured = capsys.readouterr()
    assert "PASS" in captured.out
    assert "0 issues" in captured.out


@patch("pgreviewer.cli.commands.check.asyncio.run")
def test_run_check_query_file(mock_run, tmp_path, capsys):
    """--query-file reads SQL from a file."""
    from pgreviewer.cli.commands.check import run_check

    sql_file = tmp_path / "query.sql"
    sql_file.write_text("SELECT * FROM orders")
    mock_run.return_value = []

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
