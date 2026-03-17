from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_root_dockerfile_contract() -> None:
    dockerfile = (REPO_ROOT / "Dockerfile").read_text()

    assert "FROM python:3.12-slim AS builder" in dockerfile
    assert dockerfile.count("FROM python:3.12-slim") == 2
    assert "uv pip install --system /app" in dockerfile
    assert 'ENTRYPOINT ["pgr"]' in dockerfile
    assert "org.opencontainers.image.source" in dockerfile


def test_publish_workflow_contract() -> None:
    workflow = yaml.safe_load(
        (REPO_ROOT / ".github/workflows/publish-image.yml").read_text()
    )
    on_config = workflow.get("on") or workflow.get(True)

    assert on_config["push"]["tags"] == ["*"]

    steps = workflow["jobs"]["publish"]["steps"]
    metadata_step = next(step for step in steps if step.get("id") == "meta")
    assert metadata_step["uses"] == "docker/metadata-action@v5"
    assert metadata_step["with"]["images"] == "ghcr.io/breno/pgreviewer"
