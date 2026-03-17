from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_example_workflow_contract() -> None:
    workflow = yaml.safe_load((REPO_ROOT / "docs/example-workflow.yml").read_text())
    on_config = workflow.get("on") or workflow.get(True)

    assert workflow["name"] == "pgreviewer"
    assert on_config["pull_request"]["paths"] == [
        "**.sql",
        "**/migrations/**",
        "**/models/**",
    ]

    steps = workflow["jobs"]["pgreviewer"]["steps"]
    run_step = next(
        step
        for step in steps
        if step.get("uses", "").startswith("BrenoAlberto/pgReviewer@")
    )

    assert run_step["uses"] == "BrenoAlberto/pgReviewer@v1"
    assert run_step["with"] == {
        "db_connection": "${{ secrets.PGREVIEWER_DB_URL }}",
        "llm_api_key": "${{ secrets.ANTHROPIC_API_KEY }}",
        "severity_threshold": "critical",
    }
    assert run_step["env"] == {"GITHUB_TOKEN": "${{ secrets.GITHUB_TOKEN }}"}


def test_readme_quick_start_has_exactly_three_steps_and_required_secrets() -> None:
    readme = (REPO_ROOT / "README.md").read_text()
    quick_start = readme.split("## Quick Start\n", maxsplit=1)[1].split(
        "\n## ", maxsplit=1
    )[0]

    step_headers = [
        line for line in quick_start.splitlines() if line.startswith("### ")
    ]
    assert step_headers == [
        "### 1. Copy the workflow into your repository",
        "### 2. Add required GitHub Actions secrets",
        "### 3. Open a pull request that changes SQL-related files",
    ]

    assert "PGREVIEWER_DB_URL" in quick_start
    assert "ANTHROPIC_API_KEY" in quick_start
    assert "GITHUB_TOKEN" in quick_start
