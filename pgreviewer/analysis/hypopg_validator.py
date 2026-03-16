"""HypoPG-based index validation.

Creates a hypothetical index via HypoPG, re-runs EXPLAIN to see whether
Postgres would choose it, compares costs, then cleans up.

Must be called with a connection obtained from ``write_session()`` so that
any side-effects are rolled back on exit.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from pgreviewer.analysis.explain_runner import run_explain
from pgreviewer.config import settings
from pgreviewer.infra.debug_store import HYPOPG_VALIDATION, DebugStore

if TYPE_CHECKING:
    import asyncpg

    from pgreviewer.analysis.index_suggester import IndexCandidate


class ValidationResult(BaseModel):
    """Outcome of validating a single index candidate with HypoPG."""

    cost_before: float
    cost_after: float
    improvement_pct: float
    new_plan: dict[str, Any]
    validated: bool
    rationale: str | None = None


def _build_create_index_sql(candidate: IndexCandidate) -> str:
    """Build a ``CREATE INDEX ON …`` statement from *candidate*."""
    cols = ", ".join(candidate.columns)
    sql = f"CREATE INDEX ON {candidate.table} USING {candidate.index_type} ({cols})"
    if candidate.partial_predicate:
        sql += f" WHERE {candidate.partial_predicate}"
    return sql


def _extract_root_cost(plan: dict[str, Any]) -> float:
    """Return the root ``Total Cost`` from a raw EXPLAIN JSON dict."""
    return float(plan["Plan"]["Total Cost"])


async def validate_candidate(
    candidate: IndexCandidate,
    sql: str,
    conn: asyncpg.Connection,
    run_id: str | None = None,
    debug_store: DebugStore | None = None,
) -> ValidationResult:
    """Validate *candidate* against *sql* using HypoPG on *conn*.

    Parameters
    ----------
    candidate:
        Index candidate to test.
    sql:
        Original query whose plan should improve.
    conn:
        Live database connection.  **Must** be obtained from
        :func:`pgreviewer.db.pool.write_session` so that HypoPG indexes
        are cleaned up when the session ends.

    Returns
    -------
    ValidationResult
        Contains costs before/after, improvement percentage, the new plan,
        and whether the candidate is considered validated.
    """
    # 1. Baseline cost (no hypothetical index)
    plan_before = await run_explain(sql, conn)
    cost_before = _extract_root_cost(plan_before)

    # 2. Create hypothetical index
    create_sql = _build_create_index_sql(candidate)
    row = await conn.fetchrow(
        "SELECT * FROM hypopg_create_index($1)",
        create_sql,
    )
    indexrelid: int = row["indexrelid"]

    try:
        # 3. Re-run EXPLAIN with the hypothetical index in place
        plan_after = await run_explain(sql, conn)
        cost_after = _extract_root_cost(plan_after)
    finally:
        # 4. Clean up – drop the hypothetical index regardless of errors
        await conn.execute("SELECT hypopg_drop_index($1)", indexrelid)

    # Compute improvement and decide.
    # improvement_pct can be negative if the hypothetical index makes things worse;
    # a negative value always fails the threshold check → validated=False.
    if cost_before > 0:
        improvement_pct = (cost_before - cost_after) / cost_before
    else:
        improvement_pct = 0.0

    validated = improvement_pct >= settings.HYPOPG_MIN_IMPROVEMENT
    rationale = None

    if not validated:
        if improvement_pct >= 0.05:
            rationale = (
                "Index would help slightly but may not justify the write overhead"
            )
        else:
            rationale = "No significant improvement detected"

    result = ValidationResult(
        cost_before=cost_before,
        cost_after=cost_after,
        improvement_pct=improvement_pct,
        new_plan=plan_after,
        validated=validated,
        rationale=rationale,
    )

    # 5. Log to debug store regardless of the threshold result
    if run_id and debug_store:
        debug_store.save(
            run_id,
            HYPOPG_VALIDATION,
            {
                "candidate": candidate.model_dump(),
                "sql": sql,
                "cost_before": cost_before,
                "cost_after": cost_after,
                "improvement_pct": improvement_pct,
                "validated": validated,
                "rationale": rationale,
                "plan_after": plan_after,
            },
        )

    return result
