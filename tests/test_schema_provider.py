"""Tests for the SchemaProvider protocol and implementations."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import patch

if TYPE_CHECKING:
    import pytest

from pgreviewer.core.schema_provider import (
    FileSchemaProvider,
    PgPilotSchemaProvider,
    SchemaProvider,
    resolve_schema_provider,
)

# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_file_provider_satisfies_protocol() -> None:
    provider = FileSchemaProvider(Path("/dev/null"))
    assert isinstance(provider, SchemaProvider)


def test_pgpilot_provider_satisfies_protocol() -> None:
    provider = PgPilotSchemaProvider(token="tok", url="https://example.com")
    assert isinstance(provider, SchemaProvider)


# ---------------------------------------------------------------------------
# FileSchemaProvider
# ---------------------------------------------------------------------------


def test_file_provider_name() -> None:
    assert FileSchemaProvider(Path("/dev/null")).name == "file"


def test_file_provider_delegates_to_parse_schema_file(tmp_path: Path) -> None:
    schema_file = tmp_path / "schema.sql"
    schema_file.write_text(
        "CREATE TABLE orders (id int);\n"
        "-- pgreviewer:stats "
        '{"orders":{"row_estimate":42000,"size_bytes":1024}}\n'
    )
    provider = FileSchemaProvider(schema_file)
    schema = provider.get_schema()
    assert "orders" in schema.tables
    assert schema.tables["orders"].row_estimate == 42000


# ---------------------------------------------------------------------------
# PgPilotSchemaProvider
# ---------------------------------------------------------------------------


def test_pgpilot_provider_name() -> None:
    assert PgPilotSchemaProvider(token="t", url="https://x").name == "pgpilot"


def test_pgpilot_provider_calls_api() -> None:
    payload = {"tables": {"users": {"row_estimate": 5000}}}

    class FakeResponse:
        def raise_for_status(self) -> None:
            pass

        def json(self) -> dict:
            return payload

    with patch("httpx.get", return_value=FakeResponse()) as mock_get:
        provider = PgPilotSchemaProvider(
            token="test-token", url="https://api.pgpilot.test"
        )
        schema = provider.get_schema()

    mock_get.assert_called_once_with(
        "https://api.pgpilot.test/api/v1/schema",
        headers={"Authorization": "Bearer test-token"},
        timeout=30,
    )
    assert "users" in schema.tables
    assert schema.tables["users"].row_estimate == 5000


def test_pgpilot_provider_strips_trailing_slash() -> None:
    class FakeResponse:
        def raise_for_status(self) -> None:
            pass

        def json(self) -> dict:
            return {"tables": {}}

    with patch("httpx.get", return_value=FakeResponse()) as mock_get:
        PgPilotSchemaProvider(token="t", url="https://api.pgpilot.test/").get_schema()

    assert mock_get.call_args[0][0] == "https://api.pgpilot.test/api/v1/schema"


# ---------------------------------------------------------------------------
# resolve_schema_provider
# ---------------------------------------------------------------------------


def test_resolve_pgpilot_when_token_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PGPILOT_TOKEN", "test-token")
    provider = resolve_schema_provider()
    assert provider is not None
    assert provider.name == "pgpilot"


def test_resolve_pgpilot_custom_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PGPILOT_TOKEN", "t")
    monkeypatch.setenv("PGPILOT_URL", "https://custom.host")
    provider = resolve_schema_provider()
    assert isinstance(provider, PgPilotSchemaProvider)
    assert provider._url == "https://custom.host"


def test_resolve_file_when_path_provided(tmp_path: Path) -> None:
    schema_file = tmp_path / "schema.sql"
    schema_file.write_text("CREATE TABLE t (id int);")
    provider = resolve_schema_provider(schema_path=schema_file)
    assert provider is not None
    assert provider.name == "file"


def test_resolve_auto_detects_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    pgr_dir = tmp_path / ".pgreviewer"
    pgr_dir.mkdir()
    (pgr_dir / "schema.sql").write_text("CREATE TABLE t (id int);")
    provider = resolve_schema_provider()
    assert provider is not None
    assert provider.name == "file"


def test_resolve_none_when_nothing_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("PGPILOT_TOKEN", raising=False)
    provider = resolve_schema_provider()
    assert provider is None


def test_pgpilot_takes_precedence_over_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PGPILOT_TOKEN wins even when a schema file exists."""
    monkeypatch.setenv("PGPILOT_TOKEN", "tok")
    schema_file = tmp_path / "schema.sql"
    schema_file.write_text("CREATE TABLE t (id int);")
    provider = resolve_schema_provider(schema_path=schema_file)
    assert provider is not None
    assert provider.name == "pgpilot"
