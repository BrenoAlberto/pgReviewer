"""pytest configuration for integration tests.

Handles:
- Skipping all ``@pytest.mark.integration`` tests when
  ``SKIP_INTEGRATION_TESTS=1`` is set in the environment.
"""

from __future__ import annotations

import os

import pytest


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Skip integration tests when SKIP_INTEGRATION_TESTS=1."""
    if os.environ.get("SKIP_INTEGRATION_TESTS") != "1":
        return

    skip_marker = pytest.mark.skip(reason="SKIP_INTEGRATION_TESTS=1")
    for item in items:
        if item.get_closest_marker("integration"):
            item.add_marker(skip_marker)
