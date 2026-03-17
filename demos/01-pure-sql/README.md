# Demo 01 — Pure-SQL Migrations

This demo shows pgReviewer catching a common PostgreSQL mistake: adding **foreign
key constraints without a supporting index** on the FK column.

On most databases the FK check on `INSERT`/`UPDATE`/`DELETE` triggers a
sequential scan on the referencing table unless an index exists. pgReviewer
flags this as **CRITICAL** so you catch it before the migration reaches
production.

---

## Files

| File | Purpose |
|---|---|
| `migrations/0001_initial_schema.sql` | Creates four tables with FK constraints — **no indexes on FK columns** (intentional) |
| `migrations/0002_add_missing_indexes.sql` | Corrective migration that adds the missing `CONCURRENTLY` indexes |
| `.pgreviewer.yml` | Scoped config loaded automatically when running from this directory |

---

## Expected findings

### `0001_initial_schema.sql` only

```
pgr diff --git-ref HEAD migrations/0001_initial_schema.sql
```

| Severity | Detector | Finding |
|---|---|---|
| CRITICAL | `add_foreign_key_without_index` | `orders.user_id` — no index |
| CRITICAL | `add_foreign_key_without_index` | `order_items.order_id` — no index |
| CRITICAL | `add_foreign_key_without_index` | `order_items.product_id` — no index |

### Both migrations

```
pgr diff --git-ref HEAD migrations/
```

Result: **PASS** — `0002` adds the three indexes, so all FK columns are covered.

---

## Run locally

### Prerequisites

- Docker (or a running PostgreSQL ≥ 14)
- [`uv`](https://github.com/astral-sh/uv) or a virtual environment with pgreviewer installed

### 1. Start a local database

```bash
docker run -d --name pgr-demo \
  -e POSTGRES_USER=pgr -e POSTGRES_PASSWORD=pgr -e POSTGRES_DB=demo \
  -p 5432:5432 postgres:16
```

### 2. Export the connection string

```bash
export DATABASE_URL=postgresql://pgr:pgr@127.0.0.1:5432/demo
```

### 3. Generate a diff and run pgReviewer

From the **repo root**, produce a diff covering only the first migration:

```bash
# diff against the empty tree so all lines appear as additions
git diff --no-index /dev/null demos/01-pure-sql/migrations/0001_initial_schema.sql \
  > /tmp/demo01_0001.diff || true

pgr diff /tmp/demo01_0001.diff
```

You should see **3 CRITICAL** findings for the unindexed FK columns.

Now include the corrective migration:

```bash
git diff --no-index /dev/null demos/01-pure-sql/migrations/0001_initial_schema.sql \
  > /tmp/demo01_0001.diff || true
git diff --no-index /dev/null demos/01-pure-sql/migrations/0002_add_missing_indexes.sql \
  > /tmp/demo01_0002.diff || true
cat /tmp/demo01_0001.diff /tmp/demo01_0002.diff > /tmp/demo01_both.diff

pgr diff /tmp/demo01_both.diff
```

Result: **PASS** — all FK columns are now indexed.

### 4. CI threshold enforcement

```bash
pgr diff /tmp/demo01_0001.diff --ci   # exits 1 (CRITICAL threshold violated)
pgr diff /tmp/demo01_both.diff  --ci   # exits 0 (clean)
```

---

## What the detector checks

`add_foreign_key_without_index` scans the migration for:

1. `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY (col) REFERENCES …`
2. `ALTER TABLE … ADD COLUMN col TYPE REFERENCES …`

For each FK found, it checks whether the same migration (or the existing schema)
already has an index whose leading columns match the FK columns. If not, it
emits a **CRITICAL** issue with a ready-to-copy `CREATE INDEX CONCURRENTLY`
statement in `suggested_action`.
