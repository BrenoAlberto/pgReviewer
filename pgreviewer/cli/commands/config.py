from pathlib import Path

import typer

from pgreviewer.config import ConfigError, load_pgreviewer_config

CONFIG_FILE = Path(".pgreviewer.yml")
CONFIG_TEMPLATE = """# pgReviewer project configuration
#
# Rule configuration:
# - `enabled`: enable/disable a detector by name
# - `severity`: override default severity (`info`, `warning`, `critical`)
rules:
  sequential_scan_large_table:
    enabled: true
    severity: warning
  cartesian_join:
    enabled: true

# Numeric thresholds used by detectors and recommendation logic.
thresholds:
  seq_scan_rows: 10000
  high_cost: 10000
  hypopg_min_improvement: 0.30
  large_table_ddl_rows: 10000000

# Suppressions for known-safe legacy behavior.
ignore:
  tables:
    - audit_log
    - legacy_import_*
  files:
    - migrations/seed_*
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
