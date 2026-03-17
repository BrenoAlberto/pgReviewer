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

Run with only the initial schema + queries:

```bash
# from repo root
cat > /tmp/demo06_before.diff <<'EOF'
diff --git a/demos/06-multi-tenant/migrations/0001_schema.sql b/demos/06-multi-tenant/migrations/0001_schema.sql
new file mode 100644
index 0000000..1111111
--- /dev/null
+++ b/demos/06-multi-tenant/migrations/0001_schema.sql
@@ -0,0 +1,10 @@
+CREATE TABLE events (
+    id         BIGSERIAL PRIMARY KEY,
+    tenant_id  UUID NOT NULL,
+    user_id    BIGINT NOT NULL,
+    event_type TEXT NOT NULL,
+    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
+);
+
+CREATE INDEX idx_events_user_id ON events (user_id);
diff --git a/demos/06-multi-tenant/queries/tenant_queries.sql b/demos/06-multi-tenant/queries/tenant_queries.sql
new file mode 100644
index 0000000..1111111
--- /dev/null
+++ b/demos/06-multi-tenant/queries/tenant_queries.sql
@@ -0,0 +1,15 @@
+SELECT id, event_type, created_at
+FROM events
+WHERE tenant_id = '11111111-1111-1111-1111-111111111111'
+  AND user_id = 42
+ORDER BY created_at DESC
+LIMIT 50;
+
+SELECT id, user_id, event_type
+FROM events
+WHERE tenant_id = '11111111-1111-1111-1111-111111111111'
+  AND created_at >= NOW() - INTERVAL '7 days'
+ORDER BY created_at DESC
+LIMIT 100;
+EOF
+
+pgr diff /tmp/demo06_before.diff
```

| Severity | Detector | Finding |
|---|---|---|
| CRITICAL | `sequential_scan` | Tenant-filtered query scans `events` sequentially |
| WARNING | `missing_index_on_filter` | Filter on `tenant_id` + `user_id` lacks tenant-leading composite index |

Apply the fix migration in your branch/diff and rerun. Result should be clean (or
substantially improved): composite indexes on `(tenant_id, user_id)` and
`(tenant_id, created_at)` align with the tenant query pattern.

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
