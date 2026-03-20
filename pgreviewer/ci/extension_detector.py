"""Detect Postgres extensions required by migration files.

Scans SQL and Python (Alembic) migration files for CREATE EXTENSION statements,
then maps them to apt package names so CI can install them before running migrations.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_PG_VERSION = 16

# Extensions shipped with every standard Postgres image — no apt install needed.
BUNDLED_EXTENSIONS: frozenset[str] = frozenset(
    {
        "plpgsql",
        "uuid-ossp",
        "hstore",
        "ltree",
        "citext",
        "pg_trgm",
        "tablefunc",
        "fuzzystrmatch",
        "intarray",
        "isn",
        "lo",
        "earthdistance",
        "cube",
        "dict_int",
        "dict_xsyn",
        "unaccent",
        "xml2",
        "pgcrypto",
        "pg_stat_statements",
        "pg_buffercache",
        "pgrowlocks",
        "pg_prewarm",
        "sslinfo",
        "dblink",
        "postgres_fdw",
        "file_fdw",
        "btree_gin",
        "btree_gist",
        "tcn",
        "tsm_system_rows",
        "tsm_system_time",
        "amcheck",
        "pg_visibility",
        "pageinspect",
        "pgstattuple",
        "plpython3u",
        "plperl",
        "pltcl",
        # already installed by pgReviewer's base image
        "hypopg",
    }
)

# Extension → apt package name template for Debian/Ubuntu.
# Use {pg} as a placeholder for the Postgres major version number.
# Extensions that require a custom apt repo (e.g. timescaledb, citus) are
# intentionally omitted — they end up in `unknown` and the user is told to
# provide a custom db-dockerfile-context.
EXTENSION_TO_APT_TEMPLATE: dict[str, str] = {
    "postgis": "postgresql-{pg}-postgis-3",
    "postgis_topology": "postgresql-{pg}-postgis-3",
    "postgis_raster": "postgresql-{pg}-postgis-3",
    "address_standardizer": "postgresql-{pg}-postgis-3",
    "address_standardizer_data_us": "postgresql-{pg}-postgis-3",
    "pgvector": "postgresql-{pg}-pgvector",
    "vector": "postgresql-{pg}-pgvector",
    "pgrouting": "postgresql-{pg}-pgrouting",
    "pg_partman": "postgresql-{pg}-partman",
    "rum": "postgresql-{pg}-rum",
    "orafce": "postgresql-{pg}-orafce",
    "age": "postgresql-{pg}-age",
    "pldebugger": "postgresql-{pg}-pldebugger",
    "plv8": "postgresql-{pg}-plv8",
    "pg_qualstats": "postgresql-{pg}-pg-qualstats",
    "pgtap": "pgtap",  # no version prefix on this one
}


def _resolve_apt_packages(
    extensions: set[str], pg_version: int
) -> tuple[list[str], list[str]]:
    """Return (packages_to_install, unknown_extensions) for *extensions*."""
    seen: set[str] = set()
    packages: list[str] = []
    unknown: list[str] = []

    for ext in sorted(extensions):
        if ext in BUNDLED_EXTENSIONS:
            continue
        if ext in EXTENSION_TO_APT_TEMPLATE:
            pkg = EXTENSION_TO_APT_TEMPLATE[ext].format(pg=pg_version)
            if pkg not in seen:
                packages.append(pkg)
                seen.add(pkg)
        else:
            unknown.append(ext)

    return packages, unknown

_CREATE_EXT_RE = re.compile(
    r"CREATE\s+EXTENSION\s+(?:IF\s+NOT\s+EXISTS\s+)?[\"']?(\w[\w-]*)[\"']?",
    re.IGNORECASE,
)

_MIGRATION_DIRS = {"migrations", "alembic", "versions", "flyway"}


@dataclass
class DetectionResult:
    extensions_found: set[str] = field(default_factory=set)
    packages_to_install: list[str] = field(default_factory=list)
    unknown_extensions: list[str] = field(default_factory=list)


def _is_migration_file(path: Path) -> bool:
    parts = {p.lower() for p in path.parts}
    return bool(parts & _MIGRATION_DIRS) and path.suffix in {".sql", ".py"}


def _scan_file(path: Path) -> set[str]:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return set()
    return {m.group(1).lower() for m in _CREATE_EXT_RE.finditer(text)}


def detect(
    search_root: Path, pg_version: int = DEFAULT_PG_VERSION
) -> DetectionResult:
    """Scan *search_root* for migration files and return extension requirements."""
    result = DetectionResult()

    for path in search_root.rglob("*"):
        if not path.is_file():
            continue
        if not _is_migration_file(path):
            continue
        result.extensions_found |= _scan_file(path)

    result.packages_to_install, result.unknown_extensions = _resolve_apt_packages(
        result.extensions_found, pg_version
    )
    return result


def main(argv: list[str] | None = None) -> int:
    """CLI entry point: ``pgr detect-extensions``."""
    import argparse

    parser = argparse.ArgumentParser(
        prog="pgr detect-extensions",
        description="Detect Postgres extensions required by migrations.",
    )
    parser.add_argument(
        "--path",
        default=".",
        help="Directory to scan (default: current directory).",
    )
    parser.add_argument(
        "--apt-packages",
        action="store_true",
        help="Print only the apt package names (space-separated), for scripting.",
    )
    parser.add_argument(
        "--postgres-version",
        type=int,
        default=DEFAULT_PG_VERSION,
        help=f"Postgres major version to target (default: {DEFAULT_PG_VERSION}).",
    )
    args = parser.parse_args(argv)

    result = detect(Path(args.path), pg_version=args.postgres_version)

    if result.unknown_extensions:
        print(
            "::error::pgReviewer detected extensions with no known apt pkg mapping:\n"
            + "\n".join(f"  - {e}" for e in result.unknown_extensions)
            + "\n\nProvide a custom 'db-dockerfile-context' directory whose Dockerfile "
            "installs these extensions on top of the pgReviewer base image.",
            file=sys.stderr,
        )
        return 1

    if args.apt_packages:
        print(" ".join(result.packages_to_install))
    else:
        if result.extensions_found:
            bundled = ", ".join(
                e for e in sorted(result.extensions_found) if e in BUNDLED_EXTENSIONS
            ) or "none"
            print(f"Extensions found:    {', '.join(sorted(result.extensions_found))}")
            print(f"Already bundled:     {bundled}")
            print(f"PKGs to install: {', '.join(result.packages_to_install) or 'none'}")
        else:
            print("No CREATE EXTENSION statements found in migration files.")

    return 0
