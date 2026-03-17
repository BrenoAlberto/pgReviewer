"""pytest configuration for integration tests.

Handles:
- Skipping all ``@pytest.mark.integration`` tests when
  ``SKIP_INTEGRATION_TESTS=1`` is set in the environment.
- Skipping all ``@pytest.mark.mcp`` tests when
  ``SKIP_MCP_TESTS=1`` is set in the environment.
"""

from __future__ import annotations

import os

import pytest


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Skip integration/mcp tests based on environment flags."""
    skip_integration = os.environ.get("SKIP_INTEGRATION_TESTS") == "1"
    skip_mcp = os.environ.get("SKIP_MCP_TESTS") == "1"

    if not skip_integration and not skip_mcp:
        return

    for item in items:
        if skip_integration and item.get_closest_marker("integration"):
            item.add_marker(pytest.mark.skip(reason="SKIP_INTEGRATION_TESTS=1"))
        if skip_mcp and item.get_closest_marker("mcp"):
            item.add_marker(pytest.mark.skip(reason="SKIP_MCP_TESTS=1"))
