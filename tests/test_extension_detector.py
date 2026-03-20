"""Tests for pgreviewer.ci.extension_detector."""

from __future__ import annotations

from pathlib import Path

import pytest

from pgreviewer.ci.extension_detector import (
    BUNDLED_EXTENSIONS,
    EXTENSION_TO_APT_TEMPLATE,
    DetectionResult,
    detect,
)


# ── helpers ───────────────────────────────────────────────────────────────────


def _write_migration(tmp_path: Path, filename: str, sql: str) -> Path:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir(exist_ok=True)
    p = migrations_dir / filename
    p.write_text(sql)
    return p


# ── bundled extensions are silently skipped ───────────────────────────────────


def test_bundled_extensions_produce_no_packages(tmp_path):
    _write_migration(
        tmp_path,
        "0001.sql",
        "CREATE EXTENSION IF NOT EXISTS pg_trgm;\n"
        "CREATE EXTENSION IF NOT EXISTS hstore;\n"
        "CREATE EXTENSION IF NOT EXISTS uuid-ossp;\n",
    )
    result = detect(tmp_path)
    assert result.packages_to_install == []
    assert result.unknown_extensions == []
    assert {"pg_trgm", "hstore", "uuid-ossp"}.issubset(result.extensions_found)


# ── known extensions map to version-specific apt packages ─────────────────────


@pytest.mark.parametrize("pg_version", [14, 15, 16, 17])
def test_postgis_maps_to_correct_version(tmp_path, pg_version):
    _write_migration(tmp_path, "0001.sql", "CREATE EXTENSION IF NOT EXISTS postgis;")
    result = detect(tmp_path, pg_version=pg_version)
    assert result.packages_to_install == [f"postgresql-{pg_version}-postgis-3"]
    assert result.unknown_extensions == []


@pytest.mark.parametrize("pg_version", [14, 15, 16, 17])
def test_pgvector_maps_to_correct_version(tmp_path, pg_version):
    _write_migration(tmp_path, "0001.sql", "CREATE EXTENSION IF NOT EXISTS pgvector;")
    result = detect(tmp_path, pg_version=pg_version)
    assert result.packages_to_install == [f"postgresql-{pg_version}-pgvector"]


def test_multiple_known_extensions_deduplicates_packages(tmp_path):
    # postgis_topology shares the same package as postgis
    _write_migration(
        tmp_path,
        "0001.sql",
        "CREATE EXTENSION postgis;\nCREATE EXTENSION postgis_topology;",
    )
    result = detect(tmp_path, pg_version=16)
    assert result.packages_to_install == ["postgresql-16-postgis-3"]


def test_mixed_bundled_and_known_extensions(tmp_path):
    _write_migration(
        tmp_path,
        "0001.sql",
        "CREATE EXTENSION IF NOT EXISTS pg_trgm;\n"  # bundled
        "CREATE EXTENSION IF NOT EXISTS pgvector;\n"  # needs install
        "CREATE EXTENSION IF NOT EXISTS postgis;\n",  # needs install
    )
    result = detect(tmp_path, pg_version=16)
    assert set(result.packages_to_install) == {
        "postgresql-16-pgvector",
        "postgresql-16-postgis-3",
    }
    assert result.unknown_extensions == []


# ── unknown extensions exit with error ────────────────────────────────────────


def test_unknown_extension_ends_up_in_unknown_list(tmp_path):
    _write_migration(
        tmp_path, "0001.sql", "CREATE EXTENSION IF NOT EXISTS timescaledb;"
    )
    result = detect(tmp_path, pg_version=16)
    assert "timescaledb" in result.unknown_extensions
    assert result.packages_to_install == []


# ── hypopg (already in base image) is skipped ────────────────────────────────


def test_hypopg_is_bundled_and_skipped(tmp_path):
    _write_migration(
        tmp_path, "0001.sql", "CREATE EXTENSION IF NOT EXISTS hypopg;"
    )
    result = detect(tmp_path, pg_version=16)
    assert result.packages_to_install == []
    assert result.unknown_extensions == []


# ── non-migration files are ignored ──────────────────────────────────────────


def test_non_migration_file_is_ignored(tmp_path):
    # File not in a recognised migration directory
    (tmp_path / "app.sql").write_text("CREATE EXTENSION postgis;")
    result = detect(tmp_path, pg_version=16)
    assert result.extensions_found == set()


# ── all known extension templates use the {pg} placeholder ───────────────────


def test_all_templates_are_parameterised():
    """Every template that isn't a fixed package name must contain {pg}."""
    for ext, template in EXTENSION_TO_APT_TEMPLATE.items():
        if template == "pgtap":
            continue  # intentionally version-free
        assert "{pg}" in template, (
            f"Extension '{ext}' template '{template}' is missing the {{pg}} placeholder"
        )
