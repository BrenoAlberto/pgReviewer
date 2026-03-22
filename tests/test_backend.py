from __future__ import annotations

from pgreviewer.config import Settings
from pgreviewer.core.backend import LocalBackend, get_backend


def _settings() -> Settings:
    return Settings(
        DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres",
    )


def test_get_backend_returns_local_backend() -> None:
    backend = get_backend(_settings())
    assert isinstance(backend, LocalBackend)
