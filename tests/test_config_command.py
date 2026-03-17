import re

from typer.testing import CliRunner

import pgreviewer.config as config_module
from pgreviewer.cli.main import app
from pgreviewer.core.models import Issue, Severity


def test_config_init_creates_valid_file(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()

    init_result = runner.invoke(app, ["config", "init"])
    assert init_result.exit_code == 0
    assert "Created .pgreviewer.yml" in init_result.stdout
    assert (tmp_path / ".pgreviewer.yml").exists()

    validate_result = runner.invoke(app, ["config", "validate"])
    assert validate_result.exit_code == 0
    assert "✅ Config is valid." in validate_result.stdout
    assert "0 rules disabled, 0 tables ignored." in validate_result.stdout


def test_config_init_has_comment_above_every_key(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()

    init_result = runner.invoke(app, ["config", "init"])
    assert init_result.exit_code == 0
    generated = (tmp_path / ".pgreviewer.yml").read_text(encoding="utf-8").splitlines()

    key_line_pattern = re.compile(r"^\s*[a-z_][a-z0-9_]*:\s*(?:.*)?$")
    for index, line in enumerate(generated):
        if not key_line_pattern.match(line):
            continue
        assert index > 0
        assert generated[index - 1].strip().startswith("#"), (
            f"Expected comment above key line: {line!r}"
        )


def test_config_init_prompts_before_overwrite(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / ".pgreviewer.yml"
    config_path.write_text("rules: {}\n", encoding="utf-8")
    runner = CliRunner()

    result = runner.invoke(app, ["config", "init"], input="n\n")

    assert result.exit_code == 1
    assert "already exists. Overwrite?" in result.stdout
    assert config_path.read_text(encoding="utf-8") == "rules: {}\n"


def test_config_validate_reports_threshold_type_error(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".pgreviewer.yml").write_text(
        "thresholds:\n  seq_scan_rows: not_a_number\n", encoding="utf-8"
    )
    runner = CliRunner()

    result = runner.invoke(app, ["config", "validate"])
    assert result.exit_code == 1
    assert "thresholds -> seq_scan_rows" in result.stderr


def test_config_validate_reports_all_errors(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".pgreviewer.yml").write_text(
        "unexpected: true\nthresholds:\n  seq_scan_rows: not_a_number\n",
        encoding="utf-8",
    )
    runner = CliRunner()

    result = runner.invoke(app, ["config", "validate"])

    assert result.exit_code == 1
    assert "Unknown key: unexpected" in result.stderr
    assert "thresholds -> seq_scan_rows" in result.stderr


def test_config_validate_uses_custom_config_path(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    custom = tmp_path / "custom-config.yml"
    custom.write_text("ignore:\n  tables:\n    - archive_*\n", encoding="utf-8")
    runner = CliRunner()

    result = runner.invoke(app, ["config", "validate", "--config", str(custom)])

    assert result.exit_code == 0
    assert "✅ Config is valid." in result.stdout
    assert "0 rules disabled, 1 tables ignored." in result.stdout


def test_apply_issue_config_ignores_tables_with_glob(monkeypatch) -> None:
    project_cfg = config_module.PgReviewerConfig.model_validate(
        {"ignore": {"tables": ["audit_log", "legacy_import_*"]}}
    )
    runtime_settings = config_module.settings.model_copy(
        deep=True, update={"IGNORE_TABLES": project_cfg.ignore.tables}
    )

    issues = [
        Issue(
            detector_name="sequential_scan_large_table",
            severity=Severity.WARNING,
            description="large scan",
            affected_table="audit_log",
            affected_columns=[],
            suggested_action="add index",
        ),
        Issue(
            detector_name="sequential_scan_large_table",
            severity=Severity.WARNING,
            description="legacy scan",
            affected_table="legacy_import_2020",
            affected_columns=[],
            suggested_action="add index",
        ),
        Issue(
            detector_name="sequential_scan_large_table",
            severity=Severity.WARNING,
            description="real issue",
            affected_table="orders",
            affected_columns=[],
            suggested_action="add index",
        ),
    ]

    filtered = config_module.apply_issue_config(
        issues,
        project=project_cfg,
        runtime_settings=runtime_settings,
    )

    assert len(filtered) == 1
    assert filtered[0].affected_table == "orders"


def test_apply_issue_config_supports_legacy_rule_name_alias() -> None:
    project_cfg = config_module.PgReviewerConfig.model_validate(
        {"rules": {"sequential_scan_large_table": {"enabled": False}}}
    )
    runtime_settings = config_module.settings.model_copy(deep=True)
    issue = Issue(
        detector_name="sequential_scan",
        severity=Severity.WARNING,
        description="large scan",
        affected_table="orders",
        affected_columns=[],
        suggested_action="add index",
    )

    filtered = config_module.apply_issue_config(
        [issue],
        project=project_cfg,
        runtime_settings=runtime_settings,
    )

    assert filtered == []


def test_load_runtime_config_merges_disabled_rules(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".pgreviewer.yml").write_text(
        "rules:\n  high_cost:\n    enabled: false\n",
        encoding="utf-8",
    )

    runtime_config = config_module.load_runtime_config(tmp_path / ".pgreviewer.yml")

    assert "high_cost" in runtime_config.runtime_settings.DISABLED_DETECTORS
