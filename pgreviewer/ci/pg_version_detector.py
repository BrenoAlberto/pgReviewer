"""Detect the Postgres major version used by a project.

Scans common project files (docker-compose, Dockerfiles) for Postgres version
hints so that pgReviewer's CI can select the correct base image and apt packages
without requiring users to manually specify a version.
"""

from __future__ import annotations

import re
from pathlib import Path

DEFAULT_PG_VERSION = 16

# Matches postgres image references like:
#   image: postgres:15
#   image: postgres:15.4
#   image: postgres:15.4-bullseye
#   FROM postgres:15
#   FROM postgres:15-alpine
_PG_VERSION_RE = re.compile(
    r"(?:FROM|image:)\s+(?:[a-zA-Z0-9._/-]+/)?postgres:(\d+)",
    re.IGNORECASE,
)

# docker-compose file glob patterns, in preference order
_COMPOSE_GLOBS = [
    "docker-compose.yml",
    "docker-compose.yaml",
    "docker-compose.*.yml",
    "docker-compose.*.yaml",
]

# Dockerfile glob patterns, in preference order
_DOCKERFILE_GLOBS = [
    "Dockerfile",
    "Dockerfile.*",
]


def _first_match(text: str) -> int | None:
    m = _PG_VERSION_RE.search(text)
    return int(m.group(1)) if m else None


def detect(search_root: Path) -> int | None:
    """Return the detected Postgres major version, or *None* if not found.

    Scans docker-compose files first, then Dockerfiles, and returns the first
    version found. Ignores ``postgres:latest`` and other non-numeric tags.
    """
    for glob in _COMPOSE_GLOBS:
        for path in sorted(search_root.glob(glob)):
            try:
                version = _first_match(path.read_text(encoding="utf-8", errors="ignore"))
                if version is not None:
                    return version
            except OSError:
                continue

    for glob in _DOCKERFILE_GLOBS:
        for path in sorted(search_root.glob(glob)):
            try:
                version = _first_match(path.read_text(encoding="utf-8", errors="ignore"))
                if version is not None:
                    return version
            except OSError:
                continue

    return None


def detect_or_default(search_root: Path) -> tuple[int, bool]:
    """Return ``(version, was_detected)``."""
    version = detect(search_root)
    if version is not None:
        return version, True
    return DEFAULT_PG_VERSION, False
