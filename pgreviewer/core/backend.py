from __future__ import annotations

import logging
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

if TYPE_CHECKING:
    from typing import Any

    from pgreviewer.config import Settings

logger = logging.getLogger(__name__)


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
    def __init__(self, settings: Settings | None = None):
        from pgreviewer.config import settings as default_settings

        self._settings = settings or default_settings

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
                    await schema_collector.collect_schema(
                        tables,
                        conn,
                        ignored_table_patterns=self._settings.IGNORE_TABLES,
                    )
                    if tables
                    else SchemaInfo()
                )
                from pgreviewer.analysis.issue_detectors import run_all_detectors

                issues = run_all_detectors(
                    plan,
                    schema,
                    disabled_detectors=self._settings.DISABLED_DETECTORS,
                    source_sql=query,
                )

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
            schema = await schema_collector.collect_schema(
                [table],
                conn,
                ignored_table_patterns=self._settings.IGNORE_TABLES,
            )
            return schema.tables.get(table, TableInfo())

    async def get_slow_queries(self, limit: int = 20) -> list[SlowQuery]:
        # `rows` added in PostgreSQL 13; use 0 as fallback for older versions.
        async with pool.read_session() as conn:
            pg_version: int = conn.get_server_version().major
            rows_col = "rows" if pg_version >= 13 else "0 AS rows"
            rows = await conn.fetch(
                f"""
                SELECT
                    query AS query_text,
                    calls,
                    mean_exec_time AS mean_exec_time_ms,
                    total_exec_time AS total_exec_time_ms,
                    {rows_col}
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


def get_backend(settings: Settings) -> AnalysisBackend:
    return LocalBackend(settings=settings)
