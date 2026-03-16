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

## Plan Parser

The `plan_parser` module transforms the raw Postgres JSON output into a structured, typed Pydantic tree.

### Models

The data structure is defined in `pgreviewer/core/models.py`.

- **`PlanNode`**: Represents a single node in the execution plan (e.g., `Seq Scan`, `Hash Join`). It includes fields such as `total_cost`, `startup_cost`, `plan_rows`, `filter_expr`, and a list of `children`.
- **`ExplainPlan`**: The root container for a plan, including top-level metadata like `planning_time`.
- **`IndexRecommendation`**: The final, validated output of the index analysis pipeline. It combines a candidate index with validation evidence (HypoPG cost reduction) and the ready-to-run `CREATE INDEX CONCURRENTLY` statement.

### Utilities

- **`parse_explain(raw: dict)`**: The entry point for parsing raw EXPLAIN JSON. It handles Postgres's PascalCase space-separated keys using Pydantic aliases.
- **`walk_nodes(plan: ExplainPlan)`**: A helper for depth-first traversal of the plan tree, enabling detectors to easily visit every node in the plan.

## Issue Detectors

The `issue_detectors` module implements a modular plugin architecture for analyzing execution plans and identifying performance bottlenecks.

### Architecture

- **`BaseDetector`**: An abstract base class (ABC) that all detectors must inherit from. It defines a `name` property and a `detect()` method.
- **`DetectorRegistry`**: Automatically discovers all concrete `BaseDetector` subclasses in the `pgreviewer.analysis.issue_detectors` package. It respects user configuration by filtering out detectors listed in `DISABLED_DETECTORS`.
- **`run_all_detectors()`**: A convenience function that initializes the registry, runs all enabled detectors, and aggregates the resulting `Issue` objects.

### Included Detectors

- **`sequential_scan`**: Flags sequential scans on large tables (threshold configurable via `SEQ_SCAN_ROW_THRESHOLD`).
- **`high_cost`**: Flags any query where the total plan cost exceeds `HIGH_COST_THRESHOLD`.
- **`sort_without_index`**: Flags `Sort` operations on more than 1,000 rows where the sort columns are not covered by an index on the table.
- **`cartesian_join`**: Flags Join nodes (Nested Loop, Hash Join, Merge Join) that lack a join condition, indicating a potentially dangerous CROSS JOIN.

To add a new detector, simply create a new Python file in `pgreviewer/analysis/issue_detectors/` and define a class that inherits from `BaseDetector`. It will be automatically discovered and executed during analysis.

```python
from pgreviewer.analysis.issue_detectors import BaseDetector
from pgreviewer.core.models import ExplainPlan, Issue, IssueSeverity, SchemaInfo

class MyNewDetector(BaseDetector):
    @property
    def name(self) -> str:
        return "my_new_check"

    def detect(self, plan: ExplainPlan, schema: SchemaInfo) -> list[Issue]:
        # Analysis logic here
        return []
```

## HypoPG Validation

 The `hypopg_validator` module takes suggested index candidates and verifies their impact using hypothetical indexes.

 ### Validation Workflow
 1. **Baseline**: Run `EXPLAIN` to get the current query cost.
 2. **Hypothetical Index**: Create an index using `hypopg_create_index`.
 3. **Validation**: Run `EXPLAIN` again. If Postgres chooses the hypothetical index, its cost reduction is measured.
 4. **Cleanup**: The hypothetical index is dropped immediately.

 ### Improvement Thresholds
 To avoid recommending "noisy" indexes with negligible benefits, the validator applies the following logic:
 - **Validated**: If `improvement_pct` is greater than or equal to `HYPOPG_MIN_IMPROVEMENT` (default 30%), the index is fully validated.
 - **Low Improvement**: If improvement is between 5% and the threshold, the index is not validated and a warning is issued: *"Index would help slightly but may not justify the write overhead"*.
 - **No Improvement**: If improvement is below 5%, the index is rejected.

 All validation attempts, including the before/after costs and plan shapes, are persisted to the [Debug Store](docs/debug_store.md) under the `HYPOPG_VALIDATION` category.
