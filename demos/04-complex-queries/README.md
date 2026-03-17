# Demo 04 — Complex Analytical Queries

This demo shows pgReviewer flagging **EXPLAIN-level performance issues** in
real-world query patterns:

- multi-level CTE reporting query over large tables
- window function query that sorts by an unindexed partition key
- correlated subquery filter that drives high estimated cost

---

## Files

| File | Purpose |
|---|---|
| `schema/seed.sql` | Creates and seeds large tables (50K+ rows) for meaningful EXPLAIN estimates |
| `queries/reporting_cte.sql` | Multi-CTE analytical query expected to trigger `sequential_scan` |
| `queries/window_functions.sql` | Window query with `PARTITION BY` expected to trigger `sort_without_index` |
| `queries/subquery_filter.sql` | Correlated subquery expected to trigger `high_cost` |
| `.pgreviewer.yml` | Demo-scoped severity overrides for expected detector output |

---

## Local setup (Postgres + HypoPG)

From the **repository root**:

```bash
docker compose up -d db
export DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/pgreviewer
```

Load schema and data:

```bash
psql "$DATABASE_URL" -f demos/04-complex-queries/schema/seed.sql
```

---

## Run pgReviewer on query files only

Keep seed/schema changes out of `pgr diff` input by diffing only query files:

```bash
git diff --no-index /dev/null \
  demos/04-complex-queries/queries/reporting_cte.sql \
  demos/04-complex-queries/queries/window_functions.sql \
  demos/04-complex-queries/queries/subquery_filter.sql \
  > /tmp/demo04_queries.diff || true

pgr diff /tmp/demo04_queries.diff
```

## Expected findings

| Severity | Detector | Query file |
|---|---|---|
| CRITICAL | `sequential_scan` | `reporting_cte.sql` |
| WARNING | `sort_without_index` | `window_functions.sql` |
| WARNING | `high_cost` | `subquery_filter.sql` |

---

## Why each query is flagged

- `reporting_cte.sql`: joins and aggregates over large tables (`orders`,
  `order_items`) with no supporting indexes on join/filter columns, so EXPLAIN
  includes large sequential scans.
- `window_functions.sql`: computes window metrics with
  `PARTITION BY customer_id ORDER BY occurred_at` on `event_logs` without a
  matching index, so EXPLAIN requires an explicit sort.
- `subquery_filter.sql`: uses a correlated subquery in `WHERE`, which repeats
  order counting work per customer and drives high total estimated cost.
