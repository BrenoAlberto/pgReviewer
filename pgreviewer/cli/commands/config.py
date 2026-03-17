from pathlib import Path

import typer

from pgreviewer.config import ConfigError, load_pgreviewer_config

CONFIG_FILE = Path(".pgreviewer.yml")
CONFIG_TEMPLATE = """# pgReviewer project configuration
#
# Rule configuration.
# - `enabled`: set to `false` to disable a detector.
# - `severity`: optional override (`info`, `warning`, `critical`).
# - You may add custom detector names under `rules`.
rules:
  sequential_scan:
    enabled: true
    severity: null
  missing_index_on_filter:
    enabled: true
    severity: null
  nested_loop_large_outer:
    enabled: true
    severity: null
  high_cost:
    enabled: true
    severity: null
  sort_without_index:
    enabled: true
    severity: null
  cartesian_join:
    enabled: true
    severity: null
  large_table_ddl:
    enabled: true
    severity: null
  create_index_not_concurrently:
    enabled: true
    severity: null
  alter_column_type:
    enabled: true
    severity: null
  add_column_with_default:
    enabled: true
    severity: null
  add_not_null_without_default:
    enabled: true
    severity: null
  destructive_ddl:
    enabled: true
    severity: null
  add_foreign_key_without_index:
    enabled: true
    severity: null
  drop_column_still_referenced:
    enabled: true
    severity: null
  missing_fk_index:
    enabled: true
    severity: null
  removed_index:
    enabled: true
    severity: null
  large_text_without_constraint:
    enabled: true
    severity: null
  duplicate_pk_index:
    enabled: true
    severity: null

# Numeric thresholds used by detectors and recommendation logic.
thresholds:
  seq_scan_rows: 10000
  high_cost: 10000.0
  hypopg_min_improvement: 0.30
  large_table_ddl_rows: 10000000

# Ignore lists.
# - `tables`: table-name globs excluded from analysis.
# - `files`: file-path globs excluded by diff file classifier.
# - `rules`: additional detector names to suppress.
ignore:
  tables: []
  files: []
  rules: []
"""


def run_config_init() -> None:
    CONFIG_FILE.write_text(CONFIG_TEMPLATE, encoding="utf-8")
    try:
        load_pgreviewer_config(CONFIG_FILE)
    except ConfigError as exc:
        typer.echo(f"Failed to initialize config: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Created {CONFIG_FILE}")


def run_config_validate() -> None:
    if not CONFIG_FILE.exists():
        typer.echo(f"{CONFIG_FILE} not found", err=True)
        raise typer.Exit(code=1)
    try:
        load_pgreviewer_config(CONFIG_FILE)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    typer.echo("Config is valid")
