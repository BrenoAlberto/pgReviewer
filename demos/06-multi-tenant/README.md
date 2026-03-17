# Demo 06 — Multi-tenant Composite Indexes

This demo shows pgReviewer catching one of the most common multi-tenant schema
mistakes: queries filter on `(tenant_id, user_id)`, but the table has only a
single-column index on `user_id`.

That single-column index is not tenant-leading, so plans can still degrade to
sequential scans for tenant-scoped workloads.

---

## Files

| File | Purpose |
|---|---|
| `migrations/0001_schema.sql` | Creates `events` with the **wrong** index (`user_id` only) |
| `queries/tenant_queries.sql` | Tenant-filtered queries (`tenant_id` + `user_id`, and `tenant_id` + `created_at`) |
| `migrations/0002_fix_composite_indexes.sql` | Corrective migration with tenant-leading composite indexes |
| `.pgreviewer.yml` | Scoped config to make sequential scans surface as CRITICAL in this demo |

---

## Expected findings

From the **repo root**:

```bash
git diff --no-index /dev/null demos/06-multi-tenant/migrations/0001_schema.sql \
  > /tmp/d06_schema.diff || true
git diff --no-index /dev/null demos/06-multi-tenant/queries/tenant_queries.sql \
  > /tmp/d06_queries.diff || true
cat /tmp/d06_schema.diff /tmp/d06_queries.diff > /tmp/demo06_before.diff

pgr diff /tmp/demo06_before.diff --config demos/06-multi-tenant/.pgreviewer.yml
```

| Severity | Detector | Finding |
|---|---|---|
| WARNING | `create_index_not_concurrently` | `idx_events_user_id` created without `CONCURRENTLY` |
| WARNING | `missing_index_on_filter` | Tenant time-range query scans `events` without a `(tenant_id, created_at)` index |

The `.pgreviewer.yml` sets `sequential_scan` to CRITICAL — if the analysis database has
enough rows populated (see seed steps below), `sequential_scan` will also fire for
tenant-filtered queries that cannot use the single-column `user_id` index.

HypoPG validates a `(tenant_id)` index with **~99% cost reduction** for the time-range
query. Adding the composite `(tenant_id, created_at)` index from `0002` gives even
better selectivity.

### With the fix migration

```bash
git diff --no-index /dev/null \
  demos/06-multi-tenant/migrations/0002_fix_composite_indexes.sql \
  > /tmp/d06_fix.diff || true
cat /tmp/d06_schema.diff /tmp/d06_queries.diff /tmp/d06_fix.diff \
  > /tmp/demo06_after.diff

pgr diff /tmp/demo06_after.diff --config demos/06-multi-tenant/.pgreviewer.yml
```

`0002` uses `CREATE INDEX CONCURRENTLY`, which suppresses the migration WARNING.
EXPLAIN-based findings still run against your current database schema — apply
`0002` to a staging DB and rerun `pgr check` to verify the plan improves.

---

## Multi-tenant index design pattern

For shared tables in multi-tenant systems, put `tenant_id` first in indexes used
by tenant-scoped queries.

Good patterns for this demo:

- `CREATE INDEX ... ON events (tenant_id, user_id)` for point lookups per tenant
- `CREATE INDEX ... ON events (tenant_id, created_at)` for tenant timeline/range scans

A non-tenant-leading index like `(user_id)` can still be useful for global/admin
queries, but it does not replace tenant-leading access paths for hot production
traffic.
