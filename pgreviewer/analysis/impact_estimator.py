from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pgreviewer.core.models import Issue, SchemaInfo

DEFAULT_AVG_QUERY_TIME_MS = 1.0


@dataclass(frozen=True)
class ImpactEstimate:
    max_iterations: int | None
    source_table: str | None
    source_row_estimate: int | None
    estimated_extra_queries: int | None
    estimated_time_overhead_ms: float | None
    unknown_iteration_count: bool = False
    requires_manual_review: bool = False
    summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def estimate_loop_impact(
    issue: Issue,
    schema: SchemaInfo,
    avg_query_time_ms: float = DEFAULT_AVG_QUERY_TIME_MS,
) -> ImpactEstimate:
    source_table = issue.context.get("iterable_source_table")
    if source_table is None:
        return ImpactEstimate(
            max_iterations=None,
            source_table=None,
            source_row_estimate=None,
            estimated_extra_queries=None,
            estimated_time_overhead_ms=None,
            unknown_iteration_count=True,
            requires_manual_review=True,
            summary="unknown iteration count — review manually",
        )

    table_info = schema.tables.get(source_table)
    row_estimate = table_info.row_estimate if table_info is not None else None
    if row_estimate is None:
        return ImpactEstimate(
            max_iterations=None,
            source_table=source_table,
            source_row_estimate=None,
            estimated_extra_queries=None,
            estimated_time_overhead_ms=None,
            unknown_iteration_count=True,
            requires_manual_review=True,
            summary=(
                f"{source_table} table row estimate unavailable — "
                "unknown iteration count — review manually"
            ),
        )

    estimated_time_overhead_ms = row_estimate * avg_query_time_ms
    estimated_time_overhead_seconds = estimated_time_overhead_ms / 1000
    return ImpactEstimate(
        max_iterations=row_estimate,
        source_table=source_table,
        source_row_estimate=row_estimate,
        estimated_extra_queries=row_estimate,
        estimated_time_overhead_ms=estimated_time_overhead_ms,
        summary=(
            f"{source_table} table has {row_estimate:,} rows — this loop could execute "
            f"up to {row_estimate:,} additional queries "
            f"(~{estimated_time_overhead_seconds:,.0f} seconds of DB time)"
        ),
    )
