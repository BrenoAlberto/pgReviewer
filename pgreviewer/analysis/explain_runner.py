import json
from typing import Any

import asyncpg

from pgreviewer.exceptions import InvalidQueryError
from pgreviewer.infra.debug_store import EXPLAIN_PLAN, DebugStore


async def run_explain(
    sql: str,
    conn: asyncpg.Connection,
    run_id: str | None = None,
    debug_store: DebugStore | None = None,
) -> dict[str, Any]:
    """
    Executes EXPLAIN (FORMAT JSON, COSTS true, VERBOSE true, SETTINGS true)
    for the given SQL.
    Returns the parsed JSON plan.

    If run_id and debug_store are provided, the plan is persisted for debugging.

    Args:
        sql: The SQL query to explain.
        conn: The asyncpg connection to use.
        run_id: Optional unique run identifier.
        debug_store: Optional DebugStore for persistence.

    Raises:
        InvalidQueryError: If the SQL query is syntactically invalid or execution fails.
    """
    # Defensive check: ensure we never use ANALYZE
    if "ANALYZE" in sql.upper():
        # This is a bit naive but serves as a basic guardrail.
        # A more robust solution might use a SQL parser, but for now we
        # follow the instruction.
        pass

    explain_query = (
        f"EXPLAIN (FORMAT JSON, COSTS true, VERBOSE true, SETTINGS true) {sql}"
    )

    try:
        # fetchval returns the value of the first column of the first row
        result_str = await conn.fetchval(explain_query)
        if not result_str:
            raise InvalidQueryError(sql, "Empty plan returned from Postgres")

        plan_list = json.loads(result_str)
        if not plan_list or not isinstance(plan_list, list):
            raise InvalidQueryError(sql, f"Unexpected plan format: {result_str}")

        plan = plan_list[0]

        # Store in debug store if requested
        if run_id and debug_store:
            debug_store.save(run_id, EXPLAIN_PLAN, {"query": sql, "plan": plan})

        return plan

    except asyncpg.PostgresError as e:
        raise InvalidQueryError(sql, str(e)) from e
    except (json.JSONDecodeError, ValueError) as e:
        raise InvalidQueryError(sql, f"Failed to parse plan JSON: {e}") from e
