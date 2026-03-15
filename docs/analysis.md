# Query Analysis

The analysis module is responsible for extracting and interpreting information from PostgreSQL about how queries are executed.

## Explain Runner

The `explain_runner` module provides the core functionality to retrieve execution plans.

### `run_explain(sql: str, conn: asyncpg.Connection, ...)`

This function executes an `EXPLAIN` command with the following options:
- `FORMAT JSON`: Returns the plan as a structured JSON object.
- `COSTS true`: Includes estimated startup and total costs.
- `VERBOSE true`: Includes additional information such as the output column list.
- `SETTINGS true`: Includes information about modified configuration parameters.

#### Safety Guardrails
- **No `ANALYZE`**: The runner explicitly avoids using `EXPLAIN ANALYZE` to prevent side effects and long-running executions during analysis.

#### Error Handling
- **Invalid Queries**: If a query is syntactically invalid or references missing objects, an `InvalidQueryError` is raised, containing the original SQL and the Postgres error message.

#### Debugging
If a `run_id` and `DebugStore` are provided, the raw plan is persisted under the `EXPLAIN_PLAN` category for later inspection.
