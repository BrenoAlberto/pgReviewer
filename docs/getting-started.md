# Getting Started

## Prerequisites

- Python 3.12+
- PostgreSQL 14+ with [HypoPG](https://hypopg.readthedocs.io/) extension
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Installation

```bash
pip install pgreviewer
# or
uv add pgreviewer
```

## Database setup

pgReviewer needs a PostgreSQL instance with HypoPG to validate index suggestions.
The quickest path is the bundled Docker image:

```bash
docker run -d --name pgr-db \
  -e POSTGRES_USER=pgr -e POSTGRES_PASSWORD=pgr -e POSTGRES_DB=dev \
  -p 5432:5432 ghcr.io/brenoalberto/pgreviewer-db:latest

export DATABASE_URL=postgresql://pgr:pgr@127.0.0.1:5432/dev
```

Or enable HypoPG on an existing instance:

```sql
CREATE EXTENSION IF NOT EXISTS hypopg;
```

## Analyze a single query

```bash
pgr check "SELECT * FROM orders WHERE user_id = 42"
```

pgReviewer will run `EXPLAIN`, detect any issues, suggest an index, validate it
with HypoPG, and show the before/after cost:

```
Query:   SELECT * FROM orders WHERE user_id = 42
Overall: 🟡 WARNING

 Severity   Detector              Description
 ────────────────────────────────────────────────────────────
 🟡 WARN    sequential_scan       Seq Scan on orders (150K rows)
 🟡 WARN    missing_index_on_filter  Filter on user_id, no index

💡 CREATE INDEX CONCURRENTLY idx_orders_user_id ON orders (user_id);
   Cost: 4521.00 → 8.00  (99.8% improvement via HypoPG)
```

## Analyze a diff or pull request

```bash
# Last commit
pgr diff --git-ref HEAD~1

# Compare against a branch
pgr diff --git-ref main

# Staged changes (pre-commit hook)
pgr diff --staged

# From a patch file
pgr diff /tmp/pr.diff
```

`pgr diff` finds all SQL in the changed files — migration files, `op.execute()` calls,
raw query strings in Python — and runs the full analysis pipeline on each one.

## CI mode

```bash
pgr diff --git-ref main --ci                  # exits 1 on CRITICAL (default)
pgr diff --git-ref main --ci --severity-threshold warning
pgr diff --git-ref main --json > report.json  # machine-readable output
```

## Project config

Place `.pgreviewer.yml` in your project root to tune thresholds and suppress detectors:

```yaml
rules:
  large_table_ddl:
    enabled: false
thresholds:
  seq_scan_rows: 5000
ignore:
  tables: [django_migrations, alembic_version]
```

Full reference: [configuration.md](configuration.md)

## Seeding test data

For local development against the bundled database:

```bash
pgr db seed
```

This populates `users`, `orders`, and `products` with 100K+ rows using realistic
distributions. Accurate statistics matter — PostgreSQL's cost estimates depend on them.

## Next steps

- [CI Database Setup](ci-database-setup.md) — Connect pgReviewer to a staging database in CI
- [Configuration](configuration.md) — All settings, thresholds, and suppression options
- [Issue Detectors](detectors.md) — What gets flagged and how to write custom detectors
- [Analysis Pipeline](analysis.md) — How the engine works under the hood
