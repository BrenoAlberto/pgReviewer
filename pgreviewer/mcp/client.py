from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Self

from pgreviewer.config import settings
from pgreviewer.exceptions import MCPConnectionError, MCPError, MCPTimeoutError

try:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client
except ImportError:  # pragma: no cover
    ClientSession = None
    streamablehttp_client = None

if TYPE_CHECKING:
    from types import TracebackType


class MCPClient:
    def __init__(self, server_url: str):
        self._server_url = server_url
        self._timeout_seconds = settings.MCP_TIMEOUT_SECONDS
        self._stream_context: Any = None
        self._session: Any = None

    async def connect(self) -> None:
        if self._session is not None:
            return
        if streamablehttp_client is None or ClientSession is None:
            raise MCPConnectionError(
                "Unable to connect to MCP server at "
                f"{self._server_url}: MCP SDK is not installed"
            )

        for attempt in range(3):
            try:
                self._stream_context = streamablehttp_client(self._server_url)
                read_stream, write_stream, _ = await asyncio.wait_for(
                    self._stream_context.__aenter__(),
                    timeout=self._timeout_seconds,
                )
                self._session = ClientSession(read_stream, write_stream)
                await asyncio.wait_for(
                    self._session.__aenter__(),
                    timeout=self._timeout_seconds,
                )
                await asyncio.wait_for(
                    self._session.initialize(),
                    timeout=self._timeout_seconds,
                )
                return
            except Exception as error:
                await self._cleanup()
                if self._is_transient(error) and attempt < 2:
                    await asyncio.sleep(2**attempt)
                    continue
                raise self._wrap_error(error) from error

    async def disconnect(self) -> None:
        await self._cleanup()

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        del exc_type, exc, tb
        await self.disconnect()

    async def _cleanup(self) -> None:
        cleanup_error: Exception | None = None
        if self._session is not None:
            try:
                await asyncio.wait_for(
                    self._session.__aexit__(None, None, None),
                    timeout=self._timeout_seconds,
                )
            except Exception as error:
                cleanup_error = error
            finally:
                self._session = None

        if self._stream_context is not None:
            try:
                await asyncio.wait_for(
                    self._stream_context.__aexit__(None, None, None),
                    timeout=self._timeout_seconds,
                )
            except Exception as error:
                cleanup_error = cleanup_error or error
            finally:
                self._stream_context = None

        if cleanup_error is not None:
            raise self._wrap_error(cleanup_error) from cleanup_error

    def _wrap_error(self, error: Exception) -> MCPError:
        if isinstance(error, MCPError):
            return error
        if isinstance(error, asyncio.TimeoutError):
            return MCPTimeoutError(
                f"Timed out connecting to MCP server at {self._server_url} "
                f"after {self._timeout_seconds} seconds"
            )
        if isinstance(error, (ConnectionError, OSError, ValueError)):
            return MCPConnectionError(
                f"Unable to connect to MCP server at {self._server_url}: {error}"
            )
        return MCPError(f"MCP client error for {self._server_url}: {error}")

    @staticmethod
    def _is_transient(error: Exception) -> bool:
        return isinstance(error, (asyncio.TimeoutError, ConnectionError, OSError))


def is_available(server_url: str) -> bool:
    async def _probe() -> bool:
        client = MCPClient(server_url)
        try:
            await client.connect()
            return True
        except MCPError:
            return False
        finally:
            with suppress(MCPError):
                await client.disconnect()

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(_probe())
    return False
