from typing import Any

import pytest

from pgreviewer.exceptions import ExtensionMissingError, InvalidQueryError
from pgreviewer.mcp.wrappers import (
    clear_mcp_schema_cache,
    mcp_get_explain_plan,
    mcp_get_schema_info,
    mcp_get_slow_queries,
    mcp_recommend_indexes,
)


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
    # postgres-mcp returns EXPLAIN output as a Python repr from execute_sql
    result = _FakeToolResult(
        "[{'QUERY PLAN': [{'Plan': {'Node Type': 'Seq Scan', 'Total Cost': 42.0}}]}]"
    )
    session = _FakeSession(result)
    client = _FakeClient(session)

    plan = await mcp_get_explain_plan("SELECT * FROM orders", client)

    assert plan["Plan"]["Node Type"] == "Seq Scan"
    assert session.calls == [
        (
            "execute_sql",
            {"sql": "EXPLAIN (FORMAT JSON, COSTS, VERBOSE) SELECT * FROM orders"},
        )
    ]


@pytest.mark.asyncio
async def test_mcp_get_explain_plan_passes_hypothetical_indexes():
    # Hypothetical indexes are not applied in the MCP path (postgres-mcp's
    # explain_query tool returns human-readable text, not JSON).  We always
    # call execute_sql so the caller still gets a dict with a Plan key.
    result = _FakeToolResult(
        "[{'QUERY PLAN': [{'Plan': {'Node Type': 'Index Scan', 'Total Cost': 8.0}}]}]"
    )
    session = _FakeSession(result)
    client = _FakeClient(session)
    indexes = ["CREATE INDEX ON orders (customer_id)"]

    plan = await mcp_get_explain_plan(
        "SELECT * FROM orders WHERE customer_id = 1", client, indexes
    )

    assert "Plan" in plan
    # execute_sql is used regardless of hypothetical_indexes
    assert session.calls[0][0] == "execute_sql"


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
    # postgres-mcp returns analyze_query_indexes output as Python repr text
    first_batch = _FakeToolResult(
        text=(
            "{'recommendations': ["
            "{'index_target_table': 'orders', 'index_target_columns': ('user_id',),"
            " 'benefit_of_this_index_only': {'base_cost': '100.0', 'new_cost': '60.0'},"
            " 'index_definition': 'CREATE INDEX ON orders USING btree (user_id)'}"
            "]}"
        )
    )
    second_batch = _FakeToolResult(
        text=(
            "{'recommendations': ["
            "{'index_target_table': 'orders', 'index_target_columns': ('user_id',),"
            " 'benefit_of_this_index_only': {'base_cost': '120.0', 'new_cost': '50.4'},"
            " 'index_definition': 'CREATE INDEX ON orders USING btree (user_id)'},"
            "{'index_target_table': 'orders', 'index_target_columns': ('status',),"
            " 'benefit_of_this_index_only': {'base_cost': '80.0', 'new_cost': '40.0'},"
            " 'index_definition': 'CREATE INDEX ON orders USING btree (status)'}"
            "]}"
        )
    )
    session = _FakeSession([first_batch, second_batch])
    client = _FakeClient(session)

    queries = [f"SELECT * FROM orders WHERE user_id = {i}" for i in range(15)]
    recommendations = await mcp_recommend_indexes(queries, client)

    assert len(session.calls) == 2
    assert session.calls[0] == ("analyze_query_indexes", {"queries": queries[:10]})
    assert session.calls[1] == ("analyze_query_indexes", {"queries": queries[10:]})
    assert len(recommendations) == 2

    user_id_rec = next(rec for rec in recommendations if rec.columns == ["user_id"])
    assert user_id_rec.source == "mcp_pro"
    # improvement from second batch: (120 - 50.4) / 120 ≈ 0.58
    assert user_id_rec.improvement_pct == pytest.approx(0.58, abs=0.01)


@pytest.mark.asyncio
async def test_mcp_recommend_indexes_extracts_costs_from_benefit_field():
    """Costs from analyze_query_indexes benefit_of_this_index_only are mapped."""
    result = _FakeToolResult(
        text=(
            "{'recommendations': ["
            "{'index_target_table': 'orders', 'index_target_columns': ('user_id',),"
            " 'benefit_of_this_index_only': {'base_cost': '100.0', 'new_cost': '50.0'},"
            " 'index_definition': 'CREATE INDEX ON orders USING btree (user_id)'}"
            "]}"
        )
    )
    session = _FakeSession(result)
    client = _FakeClient(session)

    recommendations = await mcp_recommend_indexes(
        ["SELECT * FROM orders WHERE user_id = 1"], client
    )

    assert len(recommendations) == 1
    rec = recommendations[0]
    assert rec.source == "mcp_pro"
    assert rec.cost_before == pytest.approx(100.0)
    assert rec.cost_after == pytest.approx(50.0)
    assert rec.improvement_pct == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_mcp_get_schema_info_maps_to_table_info():
    # postgres-mcp returns get_object_details output as Python repr text
    clear_mcp_schema_cache()
    result = _FakeToolResult(
        text=(
            "{'basic': {'schema': 'public', 'name': 'orders', 'type': 'table'},"
            " 'columns': [{'column': 'customer_id', 'data_type': 'integer',"
            "              'is_nullable': 'YES', 'default': None}],"
            " 'constraints': [],"
            " 'indexes': [{'name': 'orders_customer_id_idx',"
            "              'definition': 'CREATE INDEX orders_customer_id_idx"
            "              ON public.orders USING btree (customer_id)'}]}"
        )
    )
    session = _FakeSession(result)
    client = _FakeClient(session)

    table_info = await mcp_get_schema_info("orders", client)

    assert table_info.columns[0].name == "customer_id"
    assert table_info.indexes[0].name == "orders_customer_id_idx"
    assert session.calls == [
        ("get_object_details", {"schema_name": "public", "object_name": "orders"})
    ]


@pytest.mark.asyncio
async def test_mcp_get_schema_info_uses_cache_per_run():
    clear_mcp_schema_cache()
    result = _FakeToolResult(
        text=(
            "{'basic': {'schema': 'public', 'name': 'orders'},"
            " 'columns': [], 'indexes': []}"
        )
    )
    session = _FakeSession(result)
    client = _FakeClient(session)

    first = await mcp_get_schema_info("orders", client)
    second = await mcp_get_schema_info("orders", client)

    assert first is second
    assert session.calls == [
        ("get_object_details", {"schema_name": "public", "object_name": "orders"})
    ]


@pytest.mark.asyncio
async def test_mcp_get_slow_queries_maps_rows():
    # postgres-mcp returns get_top_queries output as Python repr text (list format)
    result = _FakeToolResult(
        text=(
            "[{'query': 'SELECT * FROM orders', 'calls': 10,"
            " 'mean_time': 12.5, 'total_time': 125.0, 'rows': 250}]"
        )
    )
    session = _FakeSession(result)
    client = _FakeClient(session)

    slow_queries = await mcp_get_slow_queries(client, limit=5)

    assert len(slow_queries) == 1
    assert slow_queries[0].query_text == "SELECT * FROM orders"
    assert slow_queries[0].calls == 10
    assert slow_queries[0].mean_exec_time_ms == pytest.approx(12.5)
    assert slow_queries[0].total_exec_time_ms == pytest.approx(125.0)
    assert slow_queries[0].rows == 250
    assert session.calls == [("get_top_queries", {"limit": 5})]


@pytest.mark.asyncio
async def test_mcp_get_slow_queries_returns_empty_when_pg_stat_statements_absent():
    result = _FakeToolResult(
        text="The pg_stat_statements extension is required to report slow queries."
    )
    session = _FakeSession(result)
    client = _FakeClient(session)

    slow_queries = await mcp_get_slow_queries(client, limit=5)

    assert slow_queries == []
