from __future__ import annotations

import ast
import re
from typing import TYPE_CHECKING, Any

from pgreviewer.core.models import (
    ColumnInfo,
    IndexInfo,
    IndexRecommendation,
    SlowQuery,
    TableInfo,
)
from pgreviewer.exceptions import (
    ExtensionMissingError,
    InvalidQueryError,
    MCPConnectionError,
    MCPError,
    MCPTimeoutError,
)

if TYPE_CHECKING:
    from pgreviewer.mcp.client import MCPClient

_MCP_MAX_QUERIES_PER_CALL = 10
_schema_cache: dict[str, TableInfo] = {}


async def mcp_get_explain_plan(
    query: str,
    conn: MCPClient,
    hypothetical_indexes: list[str] | None = None,
) -> dict[str, Any]:
    """Return an EXPLAIN (FORMAT JSON) plan dict with a top-level 'Plan' key.

    Uses ``execute_sql`` to obtain the raw PostgreSQL JSON plan.  When
    *hypothetical_indexes* are supplied they are applied via HypoPG inside
    the same execute_sql call so that the cost reflects the hypothetical index.
    """
    if hypothetical_indexes:
        return await _explain_with_hypo_indexes(query, hypothetical_indexes, conn)

    sql = f"EXPLAIN (FORMAT JSON, COSTS, VERBOSE) {query}"
    return await _run_explain_sql(sql, query, conn)


async def _explain_with_hypo_indexes(
    query: str,
    hypothetical_indexes: list[str],
    conn: MCPClient,
) -> dict[str, Any]:
    """Run EXPLAIN with hypothetical indexes.

    postgres-mcp's ``explain_query`` tool accepts hypothetical indexes as
    dicts but returns only a human-readable text plan, not the raw JSON that
    the rest of the pipeline needs.  We therefore fall back to a plain
    ``execute_sql`` EXPLAIN and accept that cost numbers will not reflect
    the hypothetical indexes in this path; ``LocalBackend`` handles the
    HypoPG-based cost comparison for ``mcp_recommend_indexes``.
    """
    return await _run_explain_sql(
        f"EXPLAIN (FORMAT JSON, COSTS, VERBOSE) {query}", query, conn
    )


async def _run_explain_sql(
    explain_sql: str,
    query: str,
    conn: MCPClient,
) -> dict[str, Any]:
    try:
        session = conn._session
        if session is None:
            await conn.connect()
            session = conn._session
        if session is None:
            raise MCPConnectionError("MCP session is not available")
        response = await session.call_tool("execute_sql", {"sql": explain_sql})
    except MCPError:
        raise
    except Exception as error:
        raise _map_tool_error(query, str(error)) from error

    if getattr(response, "isError", False):
        message = _extract_message(response)
        raise _map_tool_error(query, message)

    message = _extract_message(response)
    plan = _parse_execute_sql_explain(message)
    if plan is not None:
        return plan

    raise _map_tool_error(query, message or "Unexpected explain response from MCP")


async def mcp_recommend_indexes(
    queries: list[str],
    conn: MCPClient,
) -> list[IndexRecommendation]:
    if not queries:
        return []

    merged: dict[tuple[Any, ...], IndexRecommendation] = {}
    for i in range(0, len(queries), _MCP_MAX_QUERIES_PER_CALL):
        batch = queries[i : i + _MCP_MAX_QUERIES_PER_CALL]
        raw_recommendations = await _call_analyze_query_indexes(batch, conn)
        for raw in raw_recommendations:
            recommendation = _map_recommendation(raw)
            key = _recommendation_key(recommendation)
            existing = merged.get(key)
            if (
                existing is None
                or recommendation.improvement_pct > existing.improvement_pct
            ):
                merged[key] = recommendation

    return list(merged.values())


def clear_mcp_schema_cache() -> None:
    _schema_cache.clear()


async def mcp_get_schema_info(table: str, conn: MCPClient) -> TableInfo:
    if table in _schema_cache:
        return _schema_cache[table]

    schema_name, object_name = _split_table_name(table)
    try:
        session = conn._session
        if session is None:
            await conn.connect()
            session = conn._session
        if session is None:
            raise MCPConnectionError("MCP session is not available")
        response = await session.call_tool(
            "get_object_details",
            {"schema_name": schema_name, "object_name": object_name},
        )
    except MCPError:
        raise
    except Exception as error:
        raise _map_tool_error(table, str(error)) from error

    if getattr(response, "isError", False):
        message = _extract_message(response)
        raise _map_tool_error(table, message)

    table_info = _parse_object_details(_extract_message(response))
    _schema_cache[table] = table_info
    return table_info


async def mcp_get_slow_queries(
    conn: MCPClient,
    limit: int = 20,
) -> list[SlowQuery]:
    try:
        session = conn._session
        if session is None:
            await conn.connect()
            session = conn._session
        if session is None:
            raise MCPConnectionError("MCP session is not available")
        response = await session.call_tool("get_top_queries", {"limit": limit})
    except MCPError:
        raise
    except Exception as error:
        raise _map_tool_error("pg_stat_statements", str(error)) from error

    if getattr(response, "isError", False):
        message = _extract_message(response)
        raise _map_tool_error("pg_stat_statements", message)

    message = _extract_message(response)
    # pg_stat_statements may not be installed; treat that as an empty result.
    if "pg_stat_statements" in message.lower() and (
        "required" in message.lower() or "not" in message.lower()
    ):
        return []

    return _parse_top_queries(message)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _call_analyze_query_indexes(
    queries: list[str],
    conn: MCPClient,
) -> list[dict[str, Any]]:
    try:
        session = conn._session
        if session is None:
            await conn.connect()
            session = conn._session
        if session is None:
            raise MCPConnectionError("MCP session is not available")
        response = await session.call_tool(
            "analyze_query_indexes", {"queries": queries}
        )
    except MCPError:
        raise
    except Exception as error:
        raise _map_tool_error(";\n".join(queries), str(error)) from error

    if getattr(response, "isError", False):
        message = _extract_message(response)
        raise _map_tool_error(";\n".join(queries), message)

    message = _extract_message(response)
    return _extract_analyze_recommendations(message)


def _extract_analyze_recommendations(text: str) -> list[dict[str, Any]]:
    """Parse the Python-repr response from ``analyze_query_indexes``."""
    if not text or not text.strip():
        return []
    try:
        data = ast.literal_eval(text)
    except (ValueError, SyntaxError):
        return []
    if not isinstance(data, dict):
        return []
    recs = data.get("recommendations", [])
    if not isinstance(recs, list):
        return []
    return [r for r in recs if isinstance(r, dict)]


def _parse_execute_sql_explain(text: str) -> dict[str, Any] | None:
    """Parse an ``execute_sql`` response that contains an EXPLAIN FORMAT JSON result.

    The postgres-mcp server returns EXPLAIN output as a Python literal::

        [{'QUERY PLAN': [{'Plan': {'Node Type': 'Seq Scan', ...}}]}]
    """
    if not text or not text.strip():
        return None
    try:
        parsed = ast.literal_eval(text)
    except (ValueError, SyntaxError):
        return None
    if not isinstance(parsed, list) or not parsed:
        return None
    query_plan = parsed[0].get("QUERY PLAN")
    if not isinstance(query_plan, list) or not query_plan:
        return None
    plan_wrapper = query_plan[0]
    if isinstance(plan_wrapper, dict) and "Plan" in plan_wrapper:
        return plan_wrapper
    return None


def _parse_object_details(text: str) -> TableInfo:
    """Parse the Python-repr response from ``get_object_details``."""
    if not text or not text.strip():
        return TableInfo()
    try:
        data = ast.literal_eval(text)
    except (ValueError, SyntaxError):
        return TableInfo()
    if not isinstance(data, dict):
        return TableInfo()

    columns: list[ColumnInfo] = []
    for col in data.get("columns", []):
        if not isinstance(col, dict):
            continue
        columns.append(
            ColumnInfo(
                name=str(col.get("column") or ""),
                type=str(col.get("data_type") or ""),
            )
        )

    indexes: list[IndexInfo] = []
    for idx in data.get("indexes", []):
        if not isinstance(idx, dict):
            continue
        definition = str(idx.get("definition") or "")
        name = str(idx.get("name") or "")
        is_unique = "unique" in definition.lower()
        index_type = "btree"
        using_match = re.search(r"\bUSING\s+(\w+)", definition, re.IGNORECASE)
        if using_match:
            index_type = using_match.group(1).lower()
        cols_match = re.search(r"\(([^)]+)\)", definition)
        col_names = (
            [c.strip() for c in cols_match.group(1).split(",")] if cols_match else []
        )
        indexes.append(
            IndexInfo(
                name=name,
                columns=col_names,
                is_unique=is_unique,
                index_type=index_type,
            )
        )

    return TableInfo(columns=columns, indexes=indexes)


def _parse_top_queries(text: str) -> list[SlowQuery]:
    """Parse the Python-repr response from ``get_top_queries``."""
    if not text or not text.strip():
        return []
    try:
        data = ast.literal_eval(text)
    except (ValueError, SyntaxError):
        return []

    if isinstance(data, dict):
        data = data.get("queries") or data.get("results") or []
    if not isinstance(data, list):
        return []

    queries: list[SlowQuery] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        queries.append(
            SlowQuery(
                query_text=str(
                    item.get("query") or item.get("query_text") or item.get("sql") or ""
                ),
                calls=_to_int(item.get("calls")),
                mean_exec_time_ms=_to_float(
                    item.get("mean_exec_time_ms")
                    or item.get("mean_time")
                    or item.get("mean_exec_time")
                ),
                total_exec_time_ms=_to_float(
                    item.get("total_exec_time_ms")
                    or item.get("total_time")
                    or item.get("total_exec_time")
                ),
                rows=_to_int(item.get("rows")),
            )
        )
    return queries


def _map_recommendation(raw: dict[str, Any]) -> IndexRecommendation:
    """Map a single ``analyze_query_indexes`` recommendation dict."""
    table = str(raw.get("index_target_table") or raw.get("table") or "")

    columns_raw = raw.get("index_target_columns") or raw.get("columns") or []
    columns: list[str] = []
    if isinstance(columns_raw, list | tuple):
        columns = [str(c) for c in columns_raw if c]
    elif isinstance(columns_raw, str):
        columns = [c.strip() for c in columns_raw.split(",") if c.strip()]

    create_statement = str(
        raw.get("index_definition")
        or raw.get("create_statement")
        or raw.get("sql")
        or ""
    )
    if not create_statement and table and columns:
        create_statement = f"CREATE INDEX ON {table} USING btree ({', '.join(columns)})"

    # Extract index type from the CREATE INDEX statement
    index_type = "btree"
    using_match = re.search(r"\bUSING\s+(\w+)", create_statement, re.IGNORECASE)
    if using_match:
        index_type = using_match.group(1).lower()

    # Costs from benefit_of_this_index_only or direct fields
    benefit = raw.get("benefit_of_this_index_only") or {}
    cost_before = _to_float(raw.get("cost_before") or benefit.get("base_cost"))
    cost_after = _to_float(raw.get("cost_after") or benefit.get("new_cost"))
    improvement_pct = _to_float(raw.get("improvement_pct"))
    if improvement_pct == 0.0 and cost_before > 0.0:
        improvement_pct = (cost_before - cost_after) / cost_before

    return IndexRecommendation(
        table=table,
        columns=columns,
        index_type=index_type,
        is_unique=bool(raw.get("is_unique", False)),
        partial_predicate=None,
        create_statement=create_statement,
        cost_before=cost_before,
        cost_after=cost_after,
        improvement_pct=improvement_pct,
        validated=bool(raw.get("validated", False)),
        source="mcp_pro",
        rationale=str(raw.get("rationale") or raw.get("reason") or ""),
        notes=[str(n) for n in raw.get("notes", []) if isinstance(n, str)],
        confidence=_to_float(raw.get("confidence"), fallback=1.0),
    )


def _split_table_name(table: str) -> tuple[str, str]:
    """Split 'schema.table' into (schema, table); default schema is 'public'."""
    parts = table.split(".", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return "public", table


def _recommendation_key(rec: IndexRecommendation) -> tuple[Any, ...]:
    return (
        rec.table.lower(),
        tuple(column.lower() for column in rec.columns),
        rec.index_type.lower(),
        rec.is_unique,
        rec.partial_predicate or "",
    )


def _to_float(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _to_int(value: Any, fallback: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


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


def _map_tool_error(
    query: str, message: str
) -> MCPTimeoutError | MCPConnectionError | ExtensionMissingError | InvalidQueryError:
    lowered = message.lower()
    if "timed out" in lowered or "timeout" in lowered:
        return MCPTimeoutError(message)
    if "connect" in lowered or "connection" in lowered:
        return MCPConnectionError(message)
    if "hypopg" in lowered and ("required" in lowered or "not installed" in lowered):
        return ExtensionMissingError("hypopg")
    return InvalidQueryError(query, message)
