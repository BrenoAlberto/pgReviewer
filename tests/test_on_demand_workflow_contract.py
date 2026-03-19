"""Contract tests for the on-demand pgreviewer workflow.

These tests assert structural invariants of the workflow YAML so that
refactoring workflows never accidentally breaks the trigger logic,
acknowledgement step, or PR-head checkout.  They do NOT run actual
GitHub Actions.
"""

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOW = REPO_ROOT / ".github/workflows/pgreviewer-on-demand.yml"


def _load_workflow() -> dict:
    raw = yaml.safe_load(WORKFLOW.read_text())
    # PyYAML parses the `on:` key as boolean True in some contexts.
    on_cfg = raw.get("on") or raw.get(True)
    raw["_on"] = on_cfg
    return raw


def test_trigger_is_issue_comment_only() -> None:
    wf = _load_workflow()
    on_cfg = wf["_on"]
    assert on_cfg == {"issue_comment": {"types": ["created"]}}, (
        "Workflow must only trigger on issue_comment created events"
    )


def test_job_if_guards_pr_and_slash_command() -> None:
    wf = _load_workflow()
    job_if: str = wf["jobs"]["pgreviewer"]["if"]
    assert "github.event.issue.pull_request" in job_if, (
        "if: must check that the comment is on a PR, not a plain issue"
    )
    assert "contains(github.event.comment.body, '/pgr review')" in job_if, (
        "if: must check for the /pgr review slash command"
    )


def test_eyes_reaction_is_first_step_and_continues_on_error() -> None:
    wf = _load_workflow()
    steps = wf["jobs"]["pgreviewer"]["steps"]

    ack_idx = next(
        (
            i
            for i, s in enumerate(steps)
            if "eyes" in (s.get("name") or "").lower()
            or "eyes" in (s.get("run") or "").lower()
        ),
        None,
    )
    assert ack_idx is not None, "A step posting the 'eyes' reaction must exist"
    assert ack_idx == 0, (
        f"Eyes reaction step must be the very first step (index 0), got {ack_idx}"
    )
    assert steps[ack_idx].get("continue-on-error") is True, (
        "Eyes reaction step must have continue-on-error: true "
        "so a 👀 failure never blocks the run"
    )
    assert "github.event.comment.id" in (steps[ack_idx].get("run") or ""), (
        "Eyes reaction step must reference github.event.comment.id"
    )


def test_final_reaction_step_runs_always_and_swaps_emoji() -> None:
    wf = _load_workflow()
    steps = wf["jobs"]["pgreviewer"]["steps"]

    final_step = next(
        (
            s
            for s in steps
            if "done" in (s.get("name") or "").lower()
            or "update reaction" in (s.get("name") or "").lower()
        ),
        None,
    )
    assert final_step is not None, (
        "A final step that updates the reaction (done/update reaction) must exist"
    )
    assert final_step.get("if") == "always()", (
        "Final reaction step must have if: always() so it runs even on failure"
    )
    run: str = final_step.get("run") or ""
    assert "rocket" in run, "Final step must post 'rocket' 🚀 reaction on success"
    assert "confused" in run or "-1" in run or "DELETE" in run, (
        "Final step must remove/replace the 👀 reaction on completion"
    )


def test_fetch_pr_metadata_checks_out_pr_head() -> None:
    wf = _load_workflow()
    steps = wf["jobs"]["pgreviewer"]["steps"]
    fetch_step = next(
        (s for s in steps if "fetch pr" in (s.get("name") or "").lower()), None
    )
    assert fetch_step is not None, "Fetch PR metadata step is missing"
    run: str = fetch_step.get("run") or ""
    assert "github.event.issue.number" in run, (
        "Fetch step must use github.event.issue.number"
    )
    assert "gh pr diff $PR_NUMBER > /tmp/pr.diff" in run, (
        "Fetch step must download the PR diff"
    )
    assert 'echo "head_sha=' in run, (
        "Fetch step must export head_sha output for use in later steps"
    )
    # Must checkout the PR head so source files exist for suggestion blocks.
    assert "git checkout" in run or "git fetch" in run, (
        "Fetch step must checkout/fetch the PR head commit so source files "
        "are on disk for post_review_with_suggestions"
    )


def test_severity_step_reads_json_not_re_run_analysis() -> None:
    wf = _load_workflow()
    steps = wf["jobs"]["pgreviewer"]["steps"]
    threshold_step = next(
        (s for s in steps if "severity" in (s.get("name") or "").lower()), None
    )
    assert threshold_step is not None, "Enforce severity threshold step is missing"
    run: str = threshold_step.get("run") or ""
    assert "report.json" in run, "Severity step must read /tmp/report.json"
    assert "pgr diff" not in run, (
        "Severity step must NOT re-run pgr diff — that would run a second full analysis"
    )


def test_docs_mention_slash_command() -> None:
    readme = (REPO_ROOT / "README.md").read_text()
    docs = (REPO_ROOT / "docs/github-actions.md").read_text()

    assert "issue_comment" in readme, "README must mention the issue_comment trigger"
    assert "/pgr review" in readme, "README must document the /pgr review slash command"
    assert "/pgr review" in docs, "docs/github-actions.md must document /pgr review"
