# Demo 02 — Alembic Migrations

This demo shows pgReviewer on **Alembic-style Python migrations** common in
Flask/FastAPI/SQLAlchemy projects.

It intentionally includes:

- a foreign key added through `op.execute()` **without** an index on the FK column
- an `op.create_index()` call **without** `postgresql_concurrently=True` on a table with data

---

## Files

| File | Purpose |
|---|---|
| `alembic/versions/001_create_tables.py` | Creates tables and introduces intentional migration issues |
| `alembic/versions/002_add_indexes.py` | Corrective migration that adds the missing FK index concurrently |
| `.pgreviewer.yml` | Scoped config loaded automatically when running from this directory |

---

## Expected findings (before fix migration)

Run only the first migration:

```bash
pgr diff --git-ref HEAD alembic/versions/001_create_tables.py
```

| Severity | Detector | Finding |
|---|---|---|
| CRITICAL | `add_foreign_key_without_index` | `events.account_id` FK column without index |
| WARNING | `create_index_not_concurrently` | `op.create_index(...)` used without `postgresql_concurrently=True` |

Run both migrations:

```bash
pgr diff --git-ref HEAD alembic/versions/
```

Result: **PASS** — `002_add_indexes.py` adds the missing concurrent FK index.

---

## Alembic-specific setup

### 1. Start a local PostgreSQL database

```bash
docker run -d --name pgr-demo \
  -e POSTGRES_USER=pgr -e POSTGRES_PASSWORD=pgr -e POSTGRES_DB=demo \
  -p 5432:5432 postgres:16
```

### 2. Export connection string

```bash
export DATABASE_URL=postgresql://pgr:pgr@127.0.0.1:5432/demo
```

### 3. Generate diff and run pgReviewer

From the **repo root**:

```bash
git diff --no-index /dev/null demos/02-alembic/alembic/versions/001_create_tables.py \
  > /tmp/demo02_001.diff || true

pgr diff /tmp/demo02_001.diff
```

To include the corrective migration too:

```bash
git diff --no-index /dev/null \
  demos/02-alembic/alembic/versions/001_create_tables.py \
  demos/02-alembic/alembic/versions/002_add_indexes.py \
  > /tmp/demo02_both.diff || true

pgr diff /tmp/demo02_both.diff
```
