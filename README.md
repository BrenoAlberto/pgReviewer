<p align="center">
  <img src="docs/assets/logo.svg" alt="pgReviewer" width="88" />
</p>

<h2 align="center">pgReviewer</h2>

<p align="center">
  Automatic PostgreSQL performance review for pull requests.<br/>
  Catches slow queries, unsafe migrations, and N+1 patterns — before they reach production.
</p>

<p align="center">
  <a href="https://github.com/BrenoAlberto/pgReviewer/actions/workflows/ci.yml">
    <img src="https://github.com/BrenoAlberto/pgReviewer/actions/workflows/ci.yml/badge.svg" alt="CI"/>
  </a>
  <img src="https://img.shields.io/badge/python-3.12%2B-3b82f6" alt="Python 3.12+"/>
  <img src="https://img.shields.io/badge/PostgreSQL-14%2B-336791" alt="PostgreSQL 14+"/>
  <img src="https://img.shields.io/badge/license-MIT-22c55e" alt="MIT"/>
</p>

---

```
$ pgr diff --git-ref main

── pgReviewer Diff Analysis ───────────────────────────────────────────
File: db/migrations/0003_add_orders.sql

Line 12: ALTER TABLE orders ADD CONSTRAINT orders_user_id_fk …
Overall: 🔴 CRITICAL

 Severity   Detector                        Description
 ─────────────────────────────────────────────────────────────────────
 🔴 CRIT    add_foreign_key_without_index   FK ['user_id'] on 'orders'
                                            not indexed — causes seq
                                            scans on joins/deletes.

💡  CREATE INDEX CONCURRENTLY idx_orders_user_id ON orders (user_id);
    Cost improvement: 4 521 → 8  (99.8% via HypoPG)

Line 18: SELECT * FROM orders WHERE status = 'pending' AND user_id = $1
Overall: 🟡 WARNING

 Severity   Detector              Description
 ─────────────────────────────────────────────────────────────────────────
 🟡 WARN    sequential_scan       Seq Scan on orders (est. 142 K rows)
 🟡 WARN    missing_index_on_filter  Filter on status, no covering index

Severity threshold: critical. Found: 1 critical, 1 warning. Result: FAIL
```

## What pgReviewer catches

| Category | Detector | Notes |
|---|---|---|
| **EXPLAIN analysis** | `sequential_scan` | Seq scan on tables >10K rows |
| | `missing_index_on_filter` | Filter without supporting index |
| | `nested_loop_large_outer` | Nested loop with large outer relation |
| | `high_cost` | Query cost exceeds threshold |
| | `sort_without_index` | Sort that could use an index |
| | `cartesian_join` | Join without condition — always CRITICAL |
| **Migration safety** | `add_foreign_key_without_index` | FK without supporting index — always CRITICAL |
| | `add_not_null_without_default` | NOT NULL addition risks table lock |
| | `add_column_with_default` | Non-trivial default rewrites table (pre-PG11) |
| | `create_index_not_concurrently` | Index without `CONCURRENTLY` holds write lock |
| | `alter_column_type` | Column type change rewrites the table |
| | `destructive_ddl` | `DROP TABLE`, `DROP COLUMN`, `TRUNCATE` |
| | `large_table_ddl` | Any DDL on tables above row threshold |
| | `drop_column_referenced` | Column removed still referenced in queries |
| **Code patterns** | `query_in_loop` | N+1 — DB call inside a loop |
| | cross-file N+1 | Loop in service calls query in repository |
| | SQLAlchemy model diff | Detects missing FK indexes, removed indexes |

All findings include a ready-to-copy fix (`CREATE INDEX CONCURRENTLY …` or a two-phase migration pattern).

## Installation

```bash
pip install pgreviewer
# or
uv add pgreviewer
```

Requires PostgreSQL 14+ with the [HypoPG](https://hypopg.readthedocs.io/) extension for index validation.

## CI / GitHub Actions

Create `.github/workflows/pgreviewer.yml`:

```yaml
name: pgreviewer

on:
  pull_request:
    paths:
      - "**.sql"
      - "**/migrations/**"
      - "**/models/**/*.py"

permissions:
  contents: read
  pull-requests: write

jobs:
  review:
    runs-on: ubuntu-latest
    env:
      DATABASE_URL: postgresql://pgr:pgr@127.0.0.1:5432/review_db

    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Start Postgres + HypoPG
        run: |
          docker run -d --name pgr-db \
            -e POSTGRES_USER=pgr -e POSTGRES_PASSWORD=pgr -e POSTGRES_DB=review_db \
            -p 5432:5432 ghcr.io/brenoalberto/pgreviewer-db:latest
          for i in $(seq 1 30); do
            pg_isready -h 127.0.0.1 -p 5432 -U pgr && break; sleep 1
          done

      - uses: astral-sh/setup-uv@v5
      - run: uv sync

      - name: Download PR diff
        run: gh pr diff ${{ github.event.pull_request.number }} > /tmp/pr.diff
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}

      - name: Run pgReviewer and post comment
        run: |
          uv run pgr diff /tmp/pr.diff --json > /tmp/report.json || true
          uv run python - <<'EOF'
          import json, os
          from pathlib import Path
          from pgreviewer.reporting.diff_comment import format_diff_comment
          from pgreviewer.reporting.comment_manager import post_or_update_comment
          data = json.loads(Path("/tmp/report.json").read_text())
          post_or_update_comment(
              pr_number=int(os.environ["PR_NUMBER"]),
              repo=os.environ["GITHUB_REPOSITORY"],
              token=os.environ["GH_TOKEN"],
              body=format_diff_comment(data),
          )
          EOF
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          PR_NUMBER: ${{ github.event.pull_request.number }}
          DATABASE_URL: ${{ env.DATABASE_URL }}

      - name: Enforce severity threshold
        run: uv run pgr diff /tmp/pr.diff --ci
        env:
          DATABASE_URL: ${{ env.DATABASE_URL }}
```

For staging database connection patterns (Docker sidecar, Cloud SQL Proxy, direct) see [docs/ci-database-setup.md](docs/ci-database-setup.md).

## Local usage

```bash
export DATABASE_URL=postgresql://user:pass@localhost:5432/mydb

# Analyze a SQL query
pgr check "SELECT * FROM orders WHERE user_id = 42"

# Analyze the last commit
pgr diff --git-ref HEAD~1

# Analyze staged changes before committing
pgr diff --staged

# CI mode — exits 1 on CRITICAL findings
pgr diff --git-ref main --ci
```

## How it works

<p align="center">
  <img src="docs/assets/pipeline.svg" alt="Analysis Pipeline" width="800"/>
</p>

pgReviewer runs `EXPLAIN (FORMAT JSON, COSTS, VERBOSE)` — never `EXPLAIN ANALYZE`, never modifying your data. Index suggestions are validated with [HypoPG](https://hypopg.readthedocs.io/) by creating a hypothetical index in a read-only transaction, re-running `EXPLAIN`, measuring the cost reduction, and rolling back. The 30% improvement threshold (configurable) filters out marginal suggestions.

For complex plans (multi-join, CTEs, subqueries), an optional LLM step interprets the bottleneck and suggests remediation. It degrades gracefully — if the LLM is unavailable or over budget, the algorithmic analysis still runs.

## Configuration

`.pgreviewer.yml` in your project root:

```yaml
rules:
  sequential_scan:
    severity: warning       # override severity
  cartesian_join:
    enabled: false          # disable a detector

thresholds:
  seq_scan_rows: 5000       # flag seq scans above this row estimate
  high_cost: 5000.0         # plan cost threshold
  hypopg_min_improvement: 0.20   # minimum improvement to recommend an index

ignore:
  tables:
    - django_migrations
    - alembic_version
  files:
    - "tests/fixtures/**"
  rules:
    - large_table_ddl       # suppress for known large tables
```

Full reference: [docs/configuration.md](docs/configuration.md)

## Documentation

| | |
|---|---|
| [Getting Started](docs/getting-started.md) | Installation, first analysis, Docker setup |
| [CI Database Setup](docs/ci-database-setup.md) | Direct, Docker sidecar, Cloud SQL Proxy patterns |
| [Configuration](docs/configuration.md) | All settings and thresholds |
| [Issue Detectors](docs/detectors.md) | Detector reference and custom detector API |
| [Analysis Pipeline](docs/analysis.md) | Deep-dive into how the engine works |

## Development

```bash
uv sync

# Unit tests (no database required)
uv run pytest tests/unit -v

# Integration tests (requires DATABASE_URL)
uv run pytest -m integration

# Lint + format
uv run ruff check . && uv run ruff format .
```

See [docs/getting-started.md](docs/getting-started.md) for Docker Compose setup and MCP integration test instructions.

## License

MIT
