from typing import Any

import pytest

from pgreviewer.exceptions import ExtensionMissingError, InvalidQueryError
from pgreviewer.mcp.wrappers import mcp_get_explain_plan, mcp_recommend_indexes


class _FakeTextContent:
    def __init__(self, text: str):
        self.text = text


class _FakeToolResult:
    def __init__(
        self,
        text: str = "",
        is_error: bool = False,
        structured_content: dict[str, Any] | list[dict[str, Any]] | None = None,
    ):
        self.content = [_FakeTextContent(text)]
        self.isError = is_error
        self.structuredContent = structured_content


class _FakeSession:
    def __init__(self, result):
        self._result = result
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def call_tool(self, name: str, arguments: dict[str, Any]):
        self.calls.append((name, arguments))
        if isinstance(self._result, list):
            return self._result.pop(0)
        return self._result


class _FakeClient:
    def __init__(self, session):
        self._session = session
        self.connect_call_count = 0

    async def connect(self):
        self.connect_call_count += 1


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


@pytest.mark.asyncio
async def test_mcp_recommend_indexes_batches_and_deduplicates():
    first_batch = _FakeToolResult(
        structured_content={
            "recommendations": [
                {
                    "table": "orders",
                    "columns": ["user_id"],
                    "create_statement": "CREATE INDEX ON orders USING btree (user_id)",
                    "cost_before": 100.0,
                    "cost_after": 60.0,
                    "improvement_pct": 0.4,
                },
            ]
        }
    )
    second_batch = _FakeToolResult(
        structured_content={
            "recommendations": [
                {
                    "table": "orders",
                    "columns": ["user_id"],
                    "create_statement": "CREATE INDEX ON orders USING btree (user_id)",
                    "cost_before": 120.0,
                    "cost_after": 50.0,
                    "improvement_pct": 0.58,
                },
                {
                    "table": "orders",
                    "columns": ["status"],
                    "create_statement": "CREATE INDEX ON orders USING btree (status)",
                    "cost_before": 80.0,
                    "cost_after": 40.0,
                    "improvement_pct": 0.5,
                },
            ]
        }
    )
    session = _FakeSession([first_batch, second_batch])
    client = _FakeClient(session)

    queries = [f"SELECT * FROM orders WHERE user_id = {i}" for i in range(15)]
    recommendations = await mcp_recommend_indexes(queries, client)

    assert len(session.calls) == 2
    assert session.calls[0] == ("recommend_indexes", {"queries": queries[:10]})
    assert session.calls[1] == ("recommend_indexes", {"queries": queries[10:]})
    assert len(recommendations) == 2

    user_id_rec = next(rec for rec in recommendations if rec.columns == ["user_id"])
    assert user_id_rec.source == "mcp_pro"
    assert user_id_rec.improvement_pct == pytest.approx(0.58)


@pytest.mark.asyncio
async def test_mcp_recommend_indexes_populates_costs_with_local_validation(monkeypatch):
    result = _FakeToolResult(
        structured_content={
            "recommendations": [
                {
                    "table": "orders",
                    "columns": ["user_id"],
                    "create_statement": "CREATE INDEX ON orders USING btree (user_id)",
                }
            ]
        }
    )
    session = _FakeSession(result)
    client = _FakeClient(session)

    async def _fake_explain(
        _query: str,
        _conn: _FakeClient,
        hypothetical_indexes: list[str] | None = None,
    ) -> dict[str, Any]:
        total_cost = 50.0 if hypothetical_indexes else 100.0
        return {"Plan": {"Total Cost": total_cost}}

    monkeypatch.setattr("pgreviewer.mcp.wrappers.mcp_get_explain_plan", _fake_explain)

    recommendations = await mcp_recommend_indexes(
        ["SELECT * FROM orders WHERE user_id = 1"], client
    )

    assert len(recommendations) == 1
    rec = recommendations[0]
    assert rec.source == "mcp_pro"
    assert rec.cost_before == pytest.approx(100.0)
    assert rec.cost_after == pytest.approx(50.0)
    assert rec.improvement_pct == pytest.approx(0.5)
