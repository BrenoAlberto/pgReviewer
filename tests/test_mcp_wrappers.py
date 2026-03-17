import pytest

from pgreviewer.exceptions import ExtensionMissingError, InvalidQueryError
from pgreviewer.mcp.wrappers import mcp_get_explain_plan


class _FakeTextContent:
    def __init__(self, text: str):
        self.text = text


class _FakeToolResult:
    def __init__(self, text: str, is_error: bool = False):
        self.content = [_FakeTextContent(text)]
        self.isError = is_error


class _FakeSession:
    def __init__(self, result):
        self._result = result
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def call_tool(self, name: str, arguments: dict[str, object]):
        self.calls.append((name, arguments))
        return self._result


class _FakeClient:
    def __init__(self, session):
        self._session = session
        self.connect_calls = 0

    async def connect(self):
        self.connect_calls += 1


@pytest.mark.asyncio
async def test_mcp_get_explain_plan_returns_plan_dict_from_json_text():
    result = _FakeToolResult('{"Plan": {"Node Type": "Seq Scan"}}')
    session = _FakeSession(result)
    client = _FakeClient(session)

    plan = await mcp_get_explain_plan("SELECT * FROM orders", client)

    assert plan["Plan"]["Node Type"] == "Seq Scan"
    assert session.calls == [
        ("explain_query", {"sql": "SELECT * FROM orders", "analyze": False})
    ]


@pytest.mark.asyncio
async def test_mcp_get_explain_plan_passes_hypothetical_indexes():
    result = _FakeToolResult('{"Plan": {"Node Type": "Index Scan"}}')
    session = _FakeSession(result)
    client = _FakeClient(session)
    indexes = ["CREATE INDEX ON orders (customer_id)"]

    await mcp_get_explain_plan(
        "SELECT * FROM orders WHERE customer_id = 1", client, indexes
    )

    assert session.calls == [
        (
            "explain_query",
            {
                "sql": "SELECT * FROM orders WHERE customer_id = 1",
                "analyze": False,
                "hypothetical_indexes": indexes,
            },
        )
    ]


@pytest.mark.asyncio
async def test_mcp_get_explain_plan_maps_error_to_invalid_query_error():
    result = _FakeToolResult("Error: syntax error at or near FROM", is_error=True)
    session = _FakeSession(result)
    client = _FakeClient(session)

    with pytest.raises(InvalidQueryError, match="syntax error"):
        await mcp_get_explain_plan("SELECT FROM", client)


@pytest.mark.asyncio
async def test_mcp_get_explain_plan_maps_missing_hypopg_to_extension_error():
    result = _FakeToolResult("Error: hypopg extension is required", is_error=True)
    session = _FakeSession(result)
    client = _FakeClient(session)

    with pytest.raises(ExtensionMissingError, match="hypopg"):
        await mcp_get_explain_plan(
            "SELECT * FROM orders", client, ["CREATE INDEX ON orders (id)"]
        )
