from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from pgreviewer.exceptions import (
    ExtensionMissingError,
    InvalidQueryError,
    MCPConnectionError,
    MCPError,
    MCPTimeoutError,
)

if TYPE_CHECKING:
    from pgreviewer.mcp.client import MCPClient


async def mcp_get_explain_plan(
    query: str,
    conn: MCPClient,
    hypothetical_indexes: list[str] | None = None,
) -> dict[str, Any]:
    arguments: dict[str, Any] = {"sql": query, "analyze": False}
    if hypothetical_indexes:
        arguments["hypothetical_indexes"] = hypothetical_indexes

    try:
        if conn._session is None:
            await conn.connect()
        if conn._session is None:
            raise MCPConnectionError("MCP session is not available")
        response = await conn._session.call_tool("explain_query", arguments)
    except MCPError:
        raise
    except Exception as error:
        raise _map_tool_error(query, str(error)) from error

    if getattr(response, "isError", False):
        message = _extract_message(response)
        raise _map_tool_error(query, message)

    structured = getattr(response, "structuredContent", None)
    if isinstance(structured, dict) and "Plan" in structured:
        return structured

    message = _extract_message(response)
    plan = _extract_plan_from_text(message)
    if plan is not None:
        return plan

    raise _map_tool_error(query, message or "Unexpected explain response from MCP")


def _extract_message(response: Any) -> str:
    content = getattr(response, "content", None)
    if not isinstance(content, list):
        return ""

    parts: list[str] = []
    for item in content:
        if isinstance(item, dict):
            text = item.get("text")
        else:
            text = getattr(item, "text", None)
        if isinstance(text, str) and text:
            parts.append(text)
    return "\n".join(parts).strip()


def _extract_plan_from_text(text: str) -> dict[str, Any] | None:
    if not text:
        return None

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None

    if isinstance(parsed, dict) and "Plan" in parsed:
        return parsed
    if (
        isinstance(parsed, list)
        and parsed
        and isinstance(parsed[0], dict)
        and "Plan" in parsed[0]
    ):
        return parsed[0]
    return None


def _map_tool_error(query: str, message: str) -> Exception:
    lowered = message.lower()
    if "timed out" in lowered or "timeout" in lowered:
        return MCPTimeoutError(message)
    if "connect" in lowered or "connection" in lowered:
        return MCPConnectionError(message)
    if "hypopg" in lowered and ("required" in lowered or "not installed" in lowered):
        return ExtensionMissingError("hypopg")
    return InvalidQueryError(query, message)
