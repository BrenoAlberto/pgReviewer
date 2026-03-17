from pathlib import Path

import typer

from pgreviewer.config import ConfigError, _disabled_rules, load_pgreviewer_config

CONFIG_FILE = Path(".pgreviewer.yml")
CONFIG_TEMPLATE = """# pgReviewer project configuration
#
# Rule configuration.
# You may add custom detector names under `rules`.
# Detector configuration defaults.
rules:
  # Sequential scan detector settings.
  sequential_scan:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null
  # Missing-index-on-filter detector settings.
  missing_index_on_filter:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null
  # Nested-loop-large-outer detector settings.
  nested_loop_large_outer:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null
  # High-cost detector settings.
  high_cost:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null
  # Sort-without-index detector settings.
  sort_without_index:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null
  # Cartesian-join detector settings.
  cartesian_join:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null
  # Large-table-DDL detector settings.
  large_table_ddl:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null
  # Create-index-not-concurrently detector settings.
  create_index_not_concurrently:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null
  # Alter-column-type detector settings.
  alter_column_type:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null
  # Add-column-with-default detector settings.
  add_column_with_default:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null
  # Add-not-null-without-default detector settings.
  add_not_null_without_default:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null
  # Destructive-DDL detector settings.
  destructive_ddl:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null
  # Add-foreign-key-without-index detector settings.
  add_foreign_key_without_index:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null
  # Drop-column-still-referenced detector settings.
  drop_column_still_referenced:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null
  # Missing-fk-index detector settings.
  missing_fk_index:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null
  # Removed-index detector settings.
  removed_index:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null
  # Large-text-without-constraint detector settings.
  large_text_without_constraint:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null
  # Duplicate-primary-key-index detector settings.
  duplicate_pk_index:
    # Enable or disable this detector (default: true).
    enabled: true
    # Optional severity override: info, warning, critical (default: null).
    severity: null

# Numeric thresholds used by detectors and recommendation logic.
thresholds:
  # Minimum row count for sequential scan to be flagged as WARNING (default: 10000).
  seq_scan_rows: 10000
  # Plan cost threshold for high-cost query findings (default: 10000.0).
  high_cost: 10000.0
  # Minimum relative cost improvement to recommend an index (default: 0.30).
  hypopg_min_improvement: 0.30
  # Minimum row count for DDL-on-large-table findings (default: 10000000).
  large_table_ddl_rows: 10000000

# Ignore lists.
ignore:
  # Table-name globs excluded from analysis (default: []).
  tables: []
  # File-path globs excluded by diff file classifier (default: []).
  files: []
  # Additional detector names to suppress (default: []).
  rules: []
"""


def run_config_init(path: Path = CONFIG_FILE) -> None:
    if path.exists() and not typer.confirm(
        f"{path} already exists. Overwrite?", default=False
    ):
        raise typer.Exit(code=1)
    path.write_text(CONFIG_TEMPLATE, encoding="utf-8")
    try:
        load_pgreviewer_config(path)
    except ConfigError as exc:
        typer.echo(f"Failed to initialize config: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Created {path}")


def run_config_validate(path: Path = CONFIG_FILE) -> None:
    if not path.exists():
        typer.echo(f"{path} not found", err=True)
        raise typer.Exit(code=1)
    try:
        config = load_pgreviewer_config(path)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(
        "✅ Config is valid. "
        f"{len(_disabled_rules(config))} rules disabled, "
        f"{len(config.ignore.tables)} tables ignored."
    )
