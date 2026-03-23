"""Schema providers — unified interface for loading SchemaInfo.

Provider hierarchy (first match wins):
1. ``PGPILOT_TOKEN`` env var → :class:`PgPilotSchemaProvider` (zero-config premium)
2. Explicit ``--schema`` flag or auto-detected file → :class:`FileSchemaProvider`
3. Neither → ``None`` (detectors run in degraded-static mode)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Protocol, runtime_checkable

from pgreviewer.core.models import SchemaInfo

logger = logging.getLogger(__name__)


@runtime_checkable
class SchemaProvider(Protocol):
    """Anything that can produce a :class:`SchemaInfo`."""

    @property
    def name(self) -> str: ...

    def get_schema(self) -> SchemaInfo: ...


class FileSchemaProvider:
    """Loads schema from a ``.pgreviewer/schema.sql`` file on disk."""

    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def name(self) -> str:
        return "file"

    def get_schema(self) -> SchemaInfo:
        from pgreviewer.analysis.schema_parser import parse_schema_file

        return parse_schema_file(self._path)


class PgPilotSchemaProvider:
    """Fetches schema from the pgPilot API (zero-config premium path)."""

    def __init__(self, token: str, url: str) -> None:
        self._token = token
        self._url = url.rstrip("/")

    @property
    def name(self) -> str:
        return "pgpilot"

    def get_schema(self) -> SchemaInfo:
        import httpx

        resp = httpx.get(
            f"{self._url}/api/v1/schema",
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return SchemaInfo.model_validate(resp.json())


_DEFAULT_PGPILOT_URL = "https://api.pgpilot.dev"


def resolve_schema_provider(
    schema_path: Path | None = None,
) -> SchemaProvider | None:
    """Pick the best available schema provider.

    Returns ``None`` when no schema source is available — callers should
    fall back to an empty :class:`SchemaInfo`.
    """
    # 1. pgPilot (zero-config premium)
    token = os.environ.get("PGPILOT_TOKEN")
    if token:
        url = os.environ.get("PGPILOT_URL", _DEFAULT_PGPILOT_URL)
        logger.info("Using pgPilot schema provider (%s)", url)
        return PgPilotSchemaProvider(token, url)

    # 2. File-based (explicit path or CWD auto-detect)
    resolved = schema_path or Path(".pgreviewer/schema.sql")
    if resolved.is_file():
        logger.info("Using file schema provider (%s)", resolved)
        return FileSchemaProvider(resolved)

    # 3. No schema available
    logger.info("No schema provider available — degraded-static mode")
    return None
