from pathlib import Path

import yaml


def test_action_yml_contract() -> None:
    action = yaml.safe_load(Path("action.yml").read_text())

    assert action["name"] == "pgreviewer"
    assert (
        action["description"]
        == "Automated PostgreSQL query performance review for pull requests."
    )

    assert action["inputs"]["db_connection"] == {
        "description": "PostgreSQL connection string for the staging database.",
        "required": True,
    }
    assert action["inputs"]["config_path"] == {
        "description": "Path to .pgreviewer.yml (default: .pgreviewer.yml)",
        "required": False,
        "default": ".pgreviewer.yml",
    }
    assert action["inputs"]["severity_threshold"] == {
        "description": (
            "Fail the check on this severity or above. One of: critical, "
            "warning, info, none."
        ),
        "required": False,
        "default": "critical",
    }
    assert action["inputs"]["llm_api_key"] == {
        "description": (
            "Anthropic API key for LLM-assisted analysis. Optional — "
            "algorithmic analysis runs without it."
        ),
        "required": False,
    }
    assert action["inputs"]["mcp_server_url"] == {
        "description": "Postgres MCP Pro server URL. Optional.",
        "required": False,
    }
    assert action["inputs"]["trigger_paths"] == {
        "description": "Comma-separated glob overrides for SQL-triggered files.",
        "required": False,
        "default": "",
    }

    assert action["outputs"] == {
        "report": {"description": "Path to the generated report JSON file."},
        "outcome": {"description": "pass, fail, or warning based on findings."},
    }

    assert action["runs"] == {"using": "docker", "image": "Dockerfile"}
