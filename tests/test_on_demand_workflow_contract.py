from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_on_demand_workflow_contract() -> None:
    workflow = yaml.safe_load(
        (REPO_ROOT / ".github/workflows/pgreviewer-on-demand.yml").read_text()
    )
    # PyYAML can parse the top-level `on` key as boolean True in some contexts.
    on_config = workflow.get("on") or workflow.get(True)

    assert on_config == {"issue_comment": {"types": ["created"]}}

    job = workflow["jobs"]["pgreviewer"]
    assert "github.event.issue.pull_request != ''" in job["if"]
    assert "contains(github.event.comment.body, '/pgr review')" in job["if"]

    steps = job["steps"]
    ack_step = next(
        step for step in steps if step.get("name") == "Acknowledge trigger comment"
    )
    assert ack_step["continue-on-error"] is True
    assert "github.event.comment.id" in ack_step["run"]

    fetch_step = next(step for step in steps if step.get("name") == "Fetch PR metadata")
    assert "github.event.issue.number" in fetch_step["run"]
    assert "gh pr diff $PR_NUMBER > /tmp/pr.diff" in fetch_step["run"]


def test_on_demand_docs_contract() -> None:
    readme = (REPO_ROOT / "README.md").read_text()
    docs = (REPO_ROOT / "docs/github-actions.md").read_text()

    assert "issue_comment" in readme
    assert "/pgr review" in readme
    assert "/pgr review" in docs
