from __future__ import annotations

import pytest

from pgreviewer.exceptions import MCPConnectionError, MCPError
from pgreviewer.mcp import client as client_module
from pgreviewer.mcp.client import MCPClient, is_available


class _FakeStreamContext:
    def __init__(self) -> None:
        self.exited = False

    async def __aenter__(self):
        return object(), object(), lambda: None

    async def __aexit__(self, exc_type, exc, tb):
        del exc_type, exc, tb
        self.exited = True


class _FakeSession:
    def __init__(self, read_stream, write_stream):
        self.read_stream = read_stream
        self.write_stream = write_stream
        self.entered = False
        self.exited = False
        self.initialized = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        del exc_type, exc, tb
        self.exited = True

    async def initialize(self) -> None:
        self.initialized = True


@pytest.mark.asyncio
async def test_context_manager_connects_and_disconnects(monkeypatch):
    stream_context = _FakeStreamContext()
    session_holder: dict[str, _FakeSession] = {}

    monkeypatch.setattr(
        client_module,
        "streamablehttp_client",
        lambda _url: stream_context,
    )

    def _session_factory(read_stream, write_stream):
        session = _FakeSession(read_stream, write_stream)
        session_holder["session"] = session
        return session

    monkeypatch.setattr(client_module, "ClientSession", _session_factory)
    client = MCPClient("http://localhost:8000/mcp")

    async with client:
        session = session_holder["session"]
        assert session.entered is True
        assert session.initialized is True

    assert stream_context.exited is True
    assert session_holder["session"].exited is True


@pytest.mark.asyncio
async def test_connect_raises_connection_error_with_url(monkeypatch):
    class _BadStreamContext:
        async def __aenter__(self):
            raise ValueError("bad url")

        async def __aexit__(self, exc_type, exc, tb):
            del exc_type, exc, tb

    monkeypatch.setattr(
        client_module,
        "streamablehttp_client",
        lambda _url: _BadStreamContext(),
    )
    monkeypatch.setattr(client_module, "ClientSession", _FakeSession)
    client = MCPClient("bad-url")

    with pytest.raises(MCPConnectionError, match="bad-url"):
        await client.connect()


@pytest.mark.asyncio
async def test_connect_retries_transient_errors(monkeypatch):
    attempts = {"count": 0}
    sleep_calls: list[int] = []

    class _FlakyStreamContext(_FakeStreamContext):
        async def __aenter__(self):
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise OSError("temporary network issue")
            return await super().__aenter__()

    monkeypatch.setattr(
        client_module,
        "streamablehttp_client",
        lambda _url: _FlakyStreamContext(),
    )
    monkeypatch.setattr(client_module, "ClientSession", _FakeSession)

    async def _fake_sleep(seconds: int):
        sleep_calls.append(seconds)

    monkeypatch.setattr(client_module.asyncio, "sleep", _fake_sleep)
    client = MCPClient("http://localhost:8000/mcp")
    await client.connect()
    await client.disconnect()

    assert attempts["count"] == 3
    assert sleep_calls == [1, 2]


def test_is_available_returns_false_for_bad_url(monkeypatch):
    async def _failing_connect(self):
        raise MCPConnectionError(
            f"Unable to connect to MCP server at {self._server_url}"
        )

    async def _noop_disconnect(self):
        return None

    monkeypatch.setattr(MCPClient, "connect", _failing_connect)
    monkeypatch.setattr(MCPClient, "disconnect", _noop_disconnect)

    assert is_available("bad-url") is False


@pytest.mark.asyncio
async def test_is_available_when_event_loop_is_running(monkeypatch):
    async def _ok_connect(self):
        return None

    async def _noop_disconnect(self):
        return None

    monkeypatch.setattr(MCPClient, "connect", _ok_connect)
    monkeypatch.setattr(MCPClient, "disconnect", _noop_disconnect)

    assert is_available("http://localhost:8000/mcp") is True


@pytest.mark.asyncio
async def test_connect_wraps_unexpected_errors(monkeypatch):
    class _ExplodingStreamContext:
        async def __aenter__(self):
            raise RuntimeError("boom")

        async def __aexit__(self, exc_type, exc, tb):
            del exc_type, exc, tb

    monkeypatch.setattr(
        client_module,
        "streamablehttp_client",
        lambda _url: _ExplodingStreamContext(),
    )
    monkeypatch.setattr(client_module, "ClientSession", _FakeSession)

    client = MCPClient("http://localhost:8000/mcp")
    with pytest.raises(MCPError, match="http://localhost:8000/mcp"):
        await client.connect()
