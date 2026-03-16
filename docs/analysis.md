# Analysis Pipeline

pgReviewer's analysis engine runs a multi-stage pipeline to detect performance issues and recommend validated index improvements.

<p align="center">
  <img src="assets/pipeline.svg" alt="Analysis Pipeline" width="700" />
</p>

## Pipeline Stages

### 1. EXPLAIN Runner

**File:** `pgreviewer/analysis/explain_runner.py`

Executes `EXPLAIN` with structured output options:

```sql
EXPLAIN (FORMAT JSON, COSTS true, VERBOSE true, SETTINGS true) <query>
```

| Option | Purpose |
|--------|---------|
| `FORMAT JSON` | Structured output for programmatic parsing |
| `COSTS true` | Estimated startup and total costs |
| `VERBOSE true` | Output column lists and additional metadata |
| `SETTINGS true` | Modified configuration parameters |

**Safety:** The runner never uses `EXPLAIN ANALYZE` — no query execution, no side effects, no risk of slow queries blocking analysis.

**Error handling:** If a query is syntactically invalid or references missing objects, an `InvalidQueryError` is raised with the original SQL and the PostgreSQL error message.

### 2. Plan Parser

**File:** `pgreviewer/analysis/plan_parser.py`

Transforms raw PostgreSQL JSON output into a typed Pydantic tree:

- **`PlanNode`** — A single node in the execution plan (e.g., `Seq Scan`, `Hash Join`). Fields include `node_type`, `total_cost`, `startup_cost`, `plan_rows`, `filter_expr`, and recursive `children`.
- **`ExplainPlan`** — Root container for the plan tree.

Key utilities:
- `parse_explain(raw)` — Entry point, handles PostgreSQL's PascalCase space-separated field names via Pydantic aliases
- `walk_nodes(plan)` — Depth-first traversal generator, used by detectors to visit every node
- `extract_tables(plan)` — Collects all unique table names referenced in the plan

### 3. Schema Collector

**File:** `pgreviewer/analysis/schema_collector.py`

Queries PostgreSQL system catalogs to gather metadata about tables referenced in the plan:

| Source | Data Collected |
|--------|---------------|
| `pg_class` | Row estimates, table size |
| `pg_indexes` | Existing index definitions |
| `pg_stats` | Column-level statistics (n_distinct, null_fraction, most_common_vals) |

Results are cached per-run to avoid redundant queries. The collector outputs a `SchemaInfo` object containing `TableInfo`, `IndexInfo`, and `ColumnInfo` for each referenced table.

### 4. Issue Detectors

**File:** `pgreviewer/analysis/issue_detectors/`

See [Detectors](detectors.md) for the full reference.

Detectors analyze the parsed plan and schema metadata to identify performance issues. Each detector produces zero or more `Issue` objects with a severity, description, and suggested action.

### 5. Index Suggester

**File:** `pgreviewer/analysis/index_suggester.py`

Generates index candidates based on detected issues and schema context:

| Issue Pattern | Index Strategy |
|--------------|---------------|
| Equality filter (`WHERE col = X`) | Btree index on the column |
| Multiple equality filters | Composite index (column order matters) |
| High null fraction column | Partial index with `WHERE col IS NOT NULL` |
| Rare literal value in filter | Partial index with `WHERE col = 'value'` |
| Range filter | Btree on range column |
| Sort without index | Covering index on sort columns |

The suggester cross-references existing indexes to avoid recommending duplicates.

### 6. HypoPG Validation

**File:** `pgreviewer/analysis/hypopg_validator.py`

<p align="center">
  <img src="assets/hypopg-flow.svg" alt="HypoPG Validation Flow" width="500" />
</p>

Every index suggestion is validated using PostgreSQL's [HypoPG](https://hypopg.readthedocs.io/) extension:

1. **Baseline** — Run `EXPLAIN` to get the current query cost
2. **Create hypothetical index** — `SELECT hypopg_create_index('CREATE INDEX ON ...')` creates a virtual index (no disk I/O, no table locks)
3. **Re-run EXPLAIN** — If PostgreSQL's planner chooses the hypothetical index, measure the cost reduction
4. **Cleanup** — Drop the hypothetical index immediately

**Thresholds:**

| Improvement | Result |
|-------------|--------|
| >= 30% (configurable) | **Validated** — recommended with confidence |
| 5% – 30% | **Low improvement** — noted but not recommended ("may not justify write overhead") |
| < 5% | **Rejected** — not worth the index maintenance cost |

All operations run in a write session that is **always rolled back**, ensuring zero side effects on the database.

### 7. Index Generator

**File:** `pgreviewer/analysis/index_generator.py`

Converts validated recommendations into ready-to-run SQL:

```sql
-- Estimated cost reduction: 4521.00 → 8.00 (99.8%)
CREATE INDEX CONCURRENTLY idx_orders_user_id ON orders (user_id);
```

Features:
- `CONCURRENTLY` hint to avoid table locks in production
- Auto-naming: `idx_{table}_{columns}`, truncated to PostgreSQL's 63-character identifier limit
- `UNIQUE` detection from column statistics (`n_distinct = -1`)
- Partial index support (`WHERE` clause from the recommendation)
- Redundancy detection: flags indexes whose columns are a subset of another recommendation

## Data Flow

```
SQL string
  │
  ├─→ EXPLAIN Runner ──→ raw JSON plan
  │
  ├─→ Plan Parser ──→ ExplainPlan (PlanNode tree)
  │
  ├─→ Schema Collector ──→ SchemaInfo (tables, indexes, column stats)
  │
  ├─→ Issue Detectors ──→ list[Issue]
  │
  ├─→ Index Suggester ──→ list[IndexCandidate]
  │
  ├─→ HypoPG Validator ──→ list[IndexRecommendation] (with before/after costs)
  │
  └─→ Report (Rich CLI or JSON)
```

## Debug Artifacts

If debug storage is enabled (default), each analysis run persists:

| Artifact | Description |
|----------|-------------|
| `EXPLAIN_PLAN` | Raw PostgreSQL execution plan JSON |
| `HYPOPG_VALIDATION` | Before/after costs for each index candidate |
| `RECOMMENDATIONS` | Final processed recommendations |

See [Debug Store](debug-store.md) for storage structure and CLI usage.
