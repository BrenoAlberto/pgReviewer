"""Contract tests for the pgReviewer reusable review workflow.

These tests assert structural invariants of review.yml — the public
reusable workflow that consumers call with:

    uses: BrenoAlberto/pgReviewer/.github/workflows/review.yml@main

They ensure refactoring never silently breaks trigger logic, emoji
lifecycle, PR head checkout, or severity enforcement.
They do NOT run actual GitHub Actions.
"""

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
# review.yml is the public reusable workflow with all the logic.
WORKFLOW = REPO_ROOT / ".github/workflows/review.yml"
# pgreviewer-on-demand.yml is the thin self-review caller (dogfood).
CALLER = REPO_ROOT / ".github/workflows/pgreviewer-on-demand.yml"


def _load(path: Path) -> dict:
    raw = yaml.safe_load(path.read_text())
    on_cfg = raw.get("on") or raw.get(True)
    raw["_on"] = on_cfg
    return raw


# ── Caller (thin wrapper) ─────────────────────────────────────────────────────


def test_caller_triggers_on_issue_comment_and_pull_request() -> None:
    wf = _load(CALLER)
    on_cfg = wf["_on"]
    assert "issue_comment" in on_cfg, "Caller must trigger on issue_comment"
    assert "pull_request" in on_cfg, "Caller must trigger on pull_request"


def test_caller_delegates_to_review_yml() -> None:
    wf = _load(CALLER)
    jobs = wf["jobs"]
    delegating = any("review.yml" in (j.get("uses") or "") for j in jobs.values())
    assert delegating, "Caller must delegate to review.yml via uses: .../review.yml@..."


# ── Reusable workflow (review.yml) ───────────────────────────────────────────


def test_reusable_workflow_trigger_is_workflow_call() -> None:
    wf = _load(WORKFLOW)
    on_cfg = wf["_on"]
    assert "workflow_call" in on_cfg, (
        "review.yml must declare on: workflow_call "
        "so it can be used as a reusable workflow"
    )


def test_reusable_workflow_has_welcome_and_review_jobs() -> None:
    wf = _load(WORKFLOW)
    jobs = wf["jobs"]
    assert "welcome" in jobs, "review.yml must have a 'welcome' job for PR open events"
    assert "review" in jobs, (
        "review.yml must have a 'review' job for /pgr review events"
    )


def test_welcome_job_gates_on_pull_request_event() -> None:
    wf = _load(WORKFLOW)
    job_if: str = wf["jobs"]["welcome"].get("if") or ""
    assert "pull_request" in job_if, "welcome job must gate on pull_request event_name"


def test_review_job_guards_pr_and_slash_command() -> None:
    wf = _load(WORKFLOW)
    job_if: str = wf["jobs"]["review"].get("if") or ""
    assert "github.event.issue.pull_request" in job_if, (
        "review job if: must check comment is on a PR, not a plain issue"
    )
    assert "contains(github.event.comment.body, '/pgr review')" in job_if, (
        "review job if: must check for the /pgr review slash command"
    )


def test_eyes_reaction_is_first_step_in_review_job() -> None:
    wf = _load(WORKFLOW)
    steps = wf["jobs"]["review"]["steps"]

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
    assert ack_idx == 0, f"Eyes reaction step must be index 0 (first), got {ack_idx}"
    assert steps[ack_idx].get("continue-on-error") is True, (
        "Eyes reaction step must have continue-on-error: true "
        "so a 👀 failure never blocks the run"
    )
    assert "github.event.comment.id" in (steps[ack_idx].get("run") or ""), (
        "Eyes reaction step must reference github.event.comment.id"
    )


def test_final_reaction_step_runs_always_and_swaps_emoji() -> None:
    wf = _load(WORKFLOW)
    steps = wf["jobs"]["review"]["steps"]

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
    assert "DELETE" in run or "confused" in run, (
        "Final step must remove 👀 and replace with outcome reaction"
    )


def test_fetch_pr_metadata_checks_out_pr_head() -> None:
    wf = _load(WORKFLOW)
    steps = wf["jobs"]["review"]["steps"]
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
    assert 'echo "head_sha=' in run, "Fetch step must export head_sha output"
    assert "git checkout" in run or "git fetch" in run, (
        "Fetch step must checkout/fetch the PR head commit so source files "
        "are on disk for post_review_with_suggestions"
    )


def test_severity_step_reads_json_not_re_run_analysis() -> None:
    wf = _load(WORKFLOW)
    steps = wf["jobs"]["review"]["steps"]
    threshold_step = next(
        (s for s in steps if "severity" in (s.get("name") or "").lower()), None
    )
    assert threshold_step is not None, "Enforce severity threshold step is missing"
    run: str = threshold_step.get("run") or ""
    assert "report.json" in run, "Severity step must read /tmp/report.json"
    assert "pgr diff" not in run, (
        "Severity step must NOT re-run pgr diff — that would be a second full analysis"
    )


def test_docs_mention_slash_command() -> None:
    readme = (REPO_ROOT / "README.md").read_text()
    docs = (REPO_ROOT / "docs/github-actions.md").read_text()

    assert "/pgr review" in readme, "README must document the /pgr review slash command"
    assert "/pgr review" in docs, "docs/github-actions.md must document /pgr review"
