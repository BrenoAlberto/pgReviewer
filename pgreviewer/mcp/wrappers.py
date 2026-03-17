from __future__ import annotations

import json
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
    arguments: dict[str, Any] = {"sql": query, "analyze": False}
    if hypothetical_indexes:
        arguments["hypothetical_indexes"] = hypothetical_indexes

    try:
        session = conn._session
        if session is None:
            await conn.connect()
            session = conn._session
        if session is None:
            raise MCPConnectionError("MCP session is not available")
        response = await session.call_tool("explain_query", arguments)
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


async def mcp_recommend_indexes(
    queries: list[str],
    conn: MCPClient,
) -> list[IndexRecommendation]:
    if not queries:
        return []

    merged: dict[tuple[Any, ...], IndexRecommendation] = {}
    for i in range(0, len(queries), _MCP_MAX_QUERIES_PER_CALL):
        batch = queries[i : i + _MCP_MAX_QUERIES_PER_CALL]
        raw_recommendations = await _call_recommend_indexes(batch, conn)
        for raw in raw_recommendations:
            recommendation = _map_recommendation(raw)
            await _ensure_recommendation_costs(recommendation, batch, conn)
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

    try:
        session = conn._session
        if session is None:
            await conn.connect()
            session = conn._session
        if session is None:
            raise MCPConnectionError("MCP session is not available")
        response = await session.call_tool("get_schema_info", {"table": table})
    except MCPError:
        raise
    except Exception as error:
        raise _map_tool_error(table, str(error)) from error

    if getattr(response, "isError", False):
        message = _extract_message(response)
        raise _map_tool_error(table, message)

    table_info = _parse_table_info(
        getattr(response, "structuredContent", None) or _extract_message(response),
        table,
    )
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
        response = await session.call_tool("get_slow_queries", {"limit": limit})
    except MCPError:
        raise
    except Exception as error:
        raise _map_tool_error("pg_stat_statements", str(error)) from error

    if getattr(response, "isError", False):
        message = _extract_message(response)
        raise _map_tool_error("pg_stat_statements", message)

    return _parse_slow_queries(
        getattr(response, "structuredContent", None) or _extract_message(response)
    )


async def _call_recommend_indexes(
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
        response = await session.call_tool("recommend_indexes", {"queries": queries})
    except MCPError:
        raise
    except Exception as error:
        raise _map_tool_error(";\n".join(queries), str(error)) from error

    if getattr(response, "isError", False):
        message = _extract_message(response)
        raise _map_tool_error(";\n".join(queries), message)

    structured = getattr(response, "structuredContent", None)
    parsed = _extract_recommendations(structured)
    if parsed is not None:
        return parsed

    parsed = _extract_recommendations(_extract_message(response))
    if parsed is not None:
        return parsed

    return []


def _extract_recommendations(payload: Any) -> list[dict[str, Any]] | None:
    parsed = payload
    if isinstance(payload, str) and payload.strip():
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            return None

    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    if isinstance(parsed, dict):
        for key in ("recommendations", "indexes", "index_recommendations"):
            value = parsed.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return None


def _parse_table_info(payload: Any, table: str) -> TableInfo:
    parsed = payload
    if isinstance(payload, str) and payload.strip():
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            return TableInfo()

    if isinstance(parsed, dict):
        if isinstance(parsed.get("tables"), dict):
            maybe_table = parsed["tables"].get(table)
            if isinstance(maybe_table, dict):
                parsed = maybe_table
        elif isinstance(parsed.get(table), dict):
            parsed = parsed[table]

    if not isinstance(parsed, dict):
        return TableInfo()

    indexes: list[IndexInfo] = []
    for item in parsed.get("indexes", []):
        if not isinstance(item, dict):
            continue
        columns_raw = item.get("columns") or []
        columns = (
            [part.strip() for part in columns_raw.split(",") if part.strip()]
            if isinstance(columns_raw, str)
            else [str(col) for col in columns_raw if isinstance(col, str)]
        )
        indexes.append(
            IndexInfo(
                name=str(item.get("name") or item.get("index_name") or ""),
                columns=columns,
                is_unique=bool(item.get("is_unique", False)),
                is_partial=bool(item.get("is_partial", False)),
                index_type=str(item.get("index_type") or "btree"),
            )
        )

    columns: list[ColumnInfo] = []
    for item in parsed.get("columns", []):
        if not isinstance(item, dict):
            continue
        most_common_freqs = item.get("most_common_freqs")
        columns.append(
            ColumnInfo(
                name=str(item.get("name") or ""),
                type=str(item.get("type") or ""),
                null_fraction=_to_float(item.get("null_fraction")),
                distinct_count=_to_float(item.get("distinct_count")),
                most_common_freqs=(
                    [_to_float(value) for value in most_common_freqs]
                    if isinstance(most_common_freqs, list)
                    else []
                ),
            )
        )

    return TableInfo(
        row_estimate=_to_int(parsed.get("row_estimate")),
        size_bytes=_to_int(parsed.get("size_bytes")),
        indexes=indexes,
        columns=columns,
    )


def _parse_slow_queries(payload: Any) -> list[SlowQuery]:
    parsed = payload
    if isinstance(payload, str) and payload.strip():
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            return []

    if isinstance(parsed, dict):
        parsed = (
            parsed.get("slow_queries")
            or parsed.get("queries")
            or parsed.get("results")
            or []
        )

    if not isinstance(parsed, list):
        return []

    queries: list[SlowQuery] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        queries.append(
            SlowQuery(
                query_text=str(
                    item.get("query_text") or item.get("query") or item.get("sql") or ""
                ),
                calls=_to_int(item.get("calls")),
                mean_exec_time_ms=_to_float(
                    item.get("mean_exec_time_ms") or item.get("mean_exec_time")
                ),
                total_exec_time_ms=_to_float(
                    item.get("total_exec_time_ms") or item.get("total_exec_time")
                ),
                rows=_to_int(item.get("rows")),
            )
        )
    return queries


def _map_recommendation(raw: dict[str, Any]) -> IndexRecommendation:
    table = str(raw.get("table") or raw.get("table_name") or "")
    columns_raw = raw.get("columns") or raw.get("column_names") or []
    columns: list[str] = []
    if isinstance(columns_raw, str):
        columns = [col.strip() for col in columns_raw.split(",") if col.strip()]
    elif isinstance(columns_raw, list):
        for item in columns_raw:
            if isinstance(item, str):
                columns.append(item)
            elif isinstance(item, dict):
                name = item.get("name")
                if isinstance(name, str) and name:
                    columns.append(name)

    index_type = str(raw.get("index_type") or raw.get("type") or "btree")
    partial_predicate = raw.get("partial_predicate") or raw.get("predicate")
    create_statement = str(raw.get("create_statement") or raw.get("sql") or "")
    if not create_statement:
        create_statement = _build_create_statement(
            table=table,
            columns=columns,
            index_type=index_type,
            partial_predicate=partial_predicate,
        )

    cost_before = _to_float(raw.get("cost_before"))
    cost_after = _to_float(raw.get("cost_after"))
    improvement_pct = _to_float(raw.get("improvement_pct"))
    if improvement_pct == 0.0 and cost_before > 0.0:
        improvement_pct = (cost_before - cost_after) / cost_before

    return IndexRecommendation(
        table=table,
        columns=columns,
        index_type=index_type,
        is_unique=bool(raw.get("is_unique", False)),
        partial_predicate=(
            str(partial_predicate)
            if isinstance(partial_predicate, str) and partial_predicate
            else None
        ),
        create_statement=create_statement,
        cost_before=cost_before,
        cost_after=cost_after,
        improvement_pct=improvement_pct,
        validated=bool(raw.get("validated", False)),
        source="mcp_pro",
        rationale=str(raw.get("rationale") or raw.get("reason") or ""),
        notes=[str(note) for note in raw.get("notes", []) if isinstance(note, str)],
        confidence=_to_float(raw.get("confidence"), fallback=1.0),
    )


def _build_create_statement(
    table: str,
    columns: list[str],
    index_type: str,
    partial_predicate: Any,
) -> str:
    if not table or not columns:
        return ""
    cols = ", ".join(columns)
    statement = f"CREATE INDEX ON {table} USING {index_type} ({cols})"
    if isinstance(partial_predicate, str) and partial_predicate:
        statement += f" WHERE {partial_predicate}"
    return statement


async def _ensure_recommendation_costs(
    recommendation: IndexRecommendation,
    batch_queries: list[str],
    conn: MCPClient,
) -> None:
    if (
        recommendation.cost_before > 0
        and recommendation.cost_after > 0
        and recommendation.improvement_pct != 0
    ) or not recommendation.create_statement:
        return

    total_before = 0.0
    total_after = 0.0
    for query in batch_queries:
        plan_before = await mcp_get_explain_plan(query, conn)
        plan_after = await mcp_get_explain_plan(
            query, conn, [recommendation.create_statement]
        )
        total_before += _extract_total_cost(plan_before)
        total_after += _extract_total_cost(plan_after)

    recommendation.cost_before = total_before
    recommendation.cost_after = total_after
    if total_before > 0:
        recommendation.improvement_pct = (total_before - total_after) / total_before


def _extract_total_cost(plan: dict[str, Any]) -> float:
    raw_cost = plan.get("Plan", {}).get("Total Cost")
    return _to_float(raw_cost)


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
