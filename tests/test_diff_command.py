"""Tests for pgr diff --git-ref / --staged functionality."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pgreviewer.cli.commands.diff import _get_git_diff

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
