from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from pgreviewer.analysis import (
    explain_runner,
    hypopg_validator,
    index_generator,
    index_suggester,
    plan_parser,
    schema_collector,
)
from pgreviewer.core.models import IndexRecommendation, SchemaInfo, SlowQuery, TableInfo
from pgreviewer.db import pool
from pgreviewer.mcp.client import MCPClient
from pgreviewer.mcp.wrappers import (
    mcp_get_explain_plan,
    mcp_get_schema_info,
    mcp_get_slow_queries,
    mcp_recommend_indexes,
)

if TYPE_CHECKING:
    from typing import Any

    from pgreviewer.config import Settings


class AnalysisBackend(Protocol):
    async def get_explain_plan(
        self,
        query: str,
        hypothetical_indexes: list[str] | None = None,
    ) -> dict[str, Any]: ...

    async def recommend_indexes(
        self, queries: list[str]
    ) -> list[IndexRecommendation]: ...

    async def get_schema_info(self, table: str) -> TableInfo: ...

    async def get_slow_queries(self, limit: int = 20) -> list[SlowQuery]: ...


class LocalBackend:
    async def get_explain_plan(
        self,
        query: str,
        hypothetical_indexes: list[str] | None = None,
    ) -> dict[str, Any]:
        indexes = hypothetical_indexes or []
        if indexes:
            async with pool.write_session() as conn:
                created_ids: list[int] = []
                try:
                    for statement in indexes:
                        row = await conn.fetchrow(
                            "SELECT * FROM hypopg_create_index($1)",
                            statement,
                        )
                        created_ids.append(int(row["indexrelid"]))
                    return await explain_runner.run_explain(query, conn)
                finally:
                    for index_id in created_ids:
                        await conn.execute("SELECT hypopg_drop_index($1)", index_id)
        async with pool.read_session() as conn:
            return await explain_runner.run_explain(query, conn)

    async def recommend_indexes(self, queries: list[str]) -> list[IndexRecommendation]:
        recommendations: list[IndexRecommendation] = []
        for query in queries:
            async with pool.read_session() as conn:
                raw_plan = await explain_runner.run_explain(query, conn)
                plan = plan_parser.parse_explain(raw_plan)
                tables = plan_parser.extract_tables(plan)
                schema = (
                    await schema_collector.collect_schema(tables, conn)
                    if tables
                    else SchemaInfo()
                )
                from pgreviewer.analysis.issue_detectors import run_all_detectors

                issues = run_all_detectors(plan, schema)

            candidates = index_suggester.suggest_indexes(issues, schema)
            if not candidates:
                continue

            async with pool.write_session() as conn:
                for candidate in candidates:
                    result = await hypopg_validator.validate_candidate(
                        candidate, query, conn
                    )
                    recommendation = IndexRecommendation(
                        table=candidate.table,
                        columns=candidate.columns,
                        index_type=candidate.index_type,
                        is_unique=candidate.is_unique,
                        partial_predicate=candidate.partial_predicate,
                        cost_before=result.cost_before,
                        cost_after=result.cost_after,
                        improvement_pct=result.improvement_pct,
                        validated=result.validated,
                        rationale=result.rationale or candidate.rationale,
                    )
                    recommendation.create_statement = (
                        index_generator.generate_create_index(recommendation)
                    )
                    recommendations.append(recommendation)
                await hypopg_validator.validate_candidates_combined(
                    candidates, query, conn
                )

        recommendations.sort(key=lambda item: item.improvement_pct, reverse=True)
        return recommendations

    async def get_schema_info(self, table: str) -> TableInfo:
        async with pool.read_session() as conn:
            schema = await schema_collector.collect_schema([table], conn)
            return schema.tables.get(table, TableInfo())

    async def get_slow_queries(self, limit: int = 20) -> list[SlowQuery]:
        async with pool.read_session() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    query AS query_text,
                    calls,
                    mean_exec_time AS mean_exec_time_ms,
                    total_exec_time AS total_exec_time_ms,
                    rows
                FROM pg_stat_statements
                ORDER BY total_exec_time DESC
                LIMIT $1
                """,
                limit,
            )
        return [
            SlowQuery(
                query_text=str(row["query_text"]),
                calls=int(row["calls"]),
                mean_exec_time_ms=float(row["mean_exec_time_ms"]),
                total_exec_time_ms=float(row["total_exec_time_ms"]),
                rows=int(row["rows"]),
            )
            for row in rows
        ]


class MCPBackend:
    def __init__(self, server_url: str):
        self._server_url = server_url

    async def get_explain_plan(
        self,
        query: str,
        hypothetical_indexes: list[str] | None = None,
    ) -> dict[str, Any]:
        async with MCPClient(self._server_url) as conn:
            return await mcp_get_explain_plan(query, conn, hypothetical_indexes or [])

    async def recommend_indexes(self, queries: list[str]) -> list[IndexRecommendation]:
        async with MCPClient(self._server_url) as conn:
            return await mcp_recommend_indexes(queries, conn)

    async def get_schema_info(self, table: str) -> TableInfo:
        async with MCPClient(self._server_url) as conn:
            return await mcp_get_schema_info(table, conn)

    async def get_slow_queries(self, limit: int = 20) -> list[SlowQuery]:
        async with MCPClient(self._server_url) as conn:
            return await mcp_get_slow_queries(conn, limit=limit)


class HybridBackend:
    def __init__(self, local: LocalBackend, mcp: MCPBackend):
        self._local = local
        self._mcp = mcp

    async def get_explain_plan(
        self,
        query: str,
        hypothetical_indexes: list[str] | None = None,
    ) -> dict[str, Any]:
        return await self._local.get_explain_plan(query, hypothetical_indexes or [])

    async def recommend_indexes(self, queries: list[str]) -> list[IndexRecommendation]:
        return await self._mcp.recommend_indexes(queries)

    async def get_schema_info(self, table: str) -> TableInfo:
        return await self._mcp.get_schema_info(table)

    async def get_slow_queries(self, limit: int = 20) -> list[SlowQuery]:
        return await self._mcp.get_slow_queries(limit=limit)


def get_backend(settings: Settings) -> AnalysisBackend:
    backend = settings.BACKEND.lower()
    local = LocalBackend()
    mcp = MCPBackend(settings.MCP_SERVER_URL)
    if backend == "local":
        return local
    if backend == "mcp":
        return mcp
    if backend == "hybrid":
        return HybridBackend(local=local, mcp=mcp)
    raise ValueError(
        "Unsupported BACKEND "
        f"'{settings.BACKEND}'. Expected one of: local, mcp, hybrid."
    )
