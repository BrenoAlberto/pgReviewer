"""Stub DATABASE_URL so the parsing unit tests can run without a live database."""

from __future__ import annotations

import os


def pytest_configure(config):  # noqa: ARG001
    """Set DATABASE_URL before any module is imported."""
    os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
