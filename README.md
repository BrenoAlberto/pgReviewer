<p align="center">
  <img src="docs/assets/logo.svg" alt="pgReviewer" width="120" />
</p>

<h1 align="center">pgReviewer</h1>

<p align="center">
  <strong>Automatic PostgreSQL performance review — directly in your pull requests.</strong><br/>
  Catches slow queries, unsafe migrations, and N+1 patterns before they reach production.
</p>

<p align="center">
  <a href="https://github.com/BrenoAlberto/pgReviewer/actions/workflows/ci.yml">
    <img src="https://github.com/BrenoAlberto/pgReviewer/actions/workflows/ci.yml/badge.svg" alt="CI"/>
  </a>
  <img src="https://img.shields.io/badge/python-3.12%2B-3b82f6" alt="Python 3.12+"/>
  <img src="https://img.shields.io/badge/PostgreSQL-14%2B-336791" alt="PostgreSQL 14+"/>
  <img src="https://img.shields.io/badge/license-MIT-22c55e" alt="MIT"/>
</p>

## Real World Usage

pgReviewer posts directly to your PRs — a summary comment with all findings, plus inline review comments with copy-ready fixes at the exact line that needs attention.

| PR summary | Inline fix suggestion |
|---|---|
| ![PR Warning Summary](docs/assets/pr_warn_summary_comment.png) | ![Index Not Concurrently Warning](docs/assets/pr_idx_concurrently_comment.png) |

![Query in Loop (N+1) Detection](docs/assets/pr_query_loop_comment.png)

---

## Add to your repo in one step

Create `.github/workflows/pgreviewer.yml`:

```yaml
name: pgreviewer

on:
  pull_request:
    paths:
      # Trigger on any Python or SQL file — pgReviewer's internal classifier
      # decides what's relevant. No need to maintain a project-specific list.
      - "**.py"
      - "**.sql"

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
          fetch-depth: 1   # diff file passed to pgr — no full history needed

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - uses: docker/setup-buildx-action@v3

      - name: Start Postgres + HypoPG
        uses: docker/build-push-action@v6
        with:
          context: db/           # directory containing your Dockerfile
          load: true
          tags: pgr-db
          cache-from: type=gha
          cache-to: type=gha,mode=max

      - run: |
          docker run -d --name pgr-db \
            -e POSTGRES_USER=pgr -e POSTGRES_PASSWORD=pgr -e POSTGRES_DB=review_db \
            -p 5432:5432 pgr-db
          for i in $(seq 1 30); do
            pg_isready -h 127.0.0.1 -p 5432 -U pgr && break; sleep 1
          done

      - uses: astral-sh/setup-uv@v5
        with:
          enable-cache: true   # caches .venv keyed on uv.lock hash
      - run: uv sync

      - name: Resolve pgReviewer HEAD SHA
        id: pgr
        run: |
          echo "sha=$(git ls-remote https://github.com/BrenoAlberto/pgReviewer.git refs/heads/main | cut -f1)" >> "$GITHUB_OUTPUT"

      - uses: actions/cache@v4
        with:
          path: ~/.cache/pip
          key: pgr-${{ steps.pgr.outputs.sha }}

      - name: Install pgreviewer
        run: pip install git+https://github.com/BrenoAlberto/pgReviewer.git@${{ steps.pgr.outputs.sha }}

      - name: Download PR diff
        run: gh pr diff ${{ github.event.pull_request.number }} > /tmp/pr.diff
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}

      - name: Analyze and post comment
        run: |
          pgr diff /tmp/pr.diff --json > /tmp/report.json || true
          python - <<'EOF'
          import json, os, sys
          from pathlib import Path
          from pgreviewer.reporting.diff_comment import format_diff_comment
          from pgreviewer.reporting.comment_manager import (
              find_existing_comment,
              post_or_update_comment,
              post_review_with_suggestions,
          )

          data = json.loads(Path("/tmp/report.json").read_text())
          pr_number = int(os.environ["PR_NUMBER"])
          repo = os.environ["GITHUB_REPOSITORY"]
          token = os.environ["GH_TOKEN"]
          commit_sha = os.environ["COMMIT_SHA"]

          has_issues = (
              any(r.get("issues") for r in data.get("results", []))
              or any(e.get("model_issues") for e in data.get("model_diffs", []))
              or bool(data.get("cross_cutting_findings"))
              or bool(data.get("code_pattern_issues"))
          )
          always_comment = os.environ.get("ALWAYS_COMMENT", "").lower() in ("1", "true", "yes")
          if not has_issues and not always_comment:
              existing = find_existing_comment(pr_number=pr_number, repo=repo, token=token)
              if existing is None:
                  print("No issues found — skipping PR comment.")
                  sys.exit(0)

          post_or_update_comment(pr_number=pr_number, repo=repo, token=token,
                                 body=format_diff_comment(data))
          if has_issues:
              post_review_with_suggestions(pr_number=pr_number, repo=repo, token=token,
                                           report=data, commit_sha=commit_sha)
          EOF
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          PR_NUMBER: ${{ github.event.pull_request.number }}
          COMMIT_SHA: ${{ github.event.pull_request.head.sha }}
          DATABASE_URL: ${{ env.DATABASE_URL }}

      - name: Enforce severity threshold
        # Reads the already-generated JSON — no second analysis pass needed.
        run: |
          python - <<'EOF'
          import json, sys
          from pathlib import Path
          data = json.loads(Path("/tmp/report.json").read_text())
          criticals = (
              sum(1 for r in data.get("results", []) for i in r.get("issues", []) if i.get("severity") == "CRITICAL")
              + sum(1 for e in data.get("model_diffs", []) for i in e.get("model_issues", []) if i.get("severity") == "CRITICAL")
              + sum(1 for f in data.get("cross_cutting_findings", []) if f.get("severity") == "CRITICAL")
              + sum(1 for i in data.get("code_pattern_issues", []) if i.get("severity") == "CRITICAL")
          )
          print(f"Severity threshold: critical. Found: {criticals} critical. Result: {'FAIL' if criticals else 'PASS'}")
          sys.exit(1 if criticals else 0)
          EOF
```

Every PR that touches SQL, migrations, or model files gets an automatic review comment and a ✅ / ❌ check status. No manual steps, no review fatigue.

For staging database connection patterns (Docker sidecar, Cloud SQL Proxy, direct) see [docs/ci-database-setup.md](docs/ci-database-setup.md).

### Always-comment mode (optional)

By default pgReviewer is silent on PRs that have never had any findings — it only posts when there are issues, and updates to a ✅ pass state when existing findings are resolved. This avoids comment noise on PRs that touch Python or SQL files but have no database interaction.

Set `ALWAYS_COMMENT: "true"` in your workflow env to post a status comment on every PR regardless:

```yaml
jobs:
  review:
    env:
      ALWAYS_COMMENT: "true"   # useful for test-beds where silence = ambiguity
```

This makes it possible to tell "pgReviewer ran and found nothing" apart from "pgReviewer didn't run at all".

---

### Branded bot identity (optional)

By default comments are posted as `github-actions[bot]`. To show `pgr[bot]` (or your own app name) with a custom avatar instead, create a [GitHub App](https://github.com/settings/apps/new) with `Pull requests: Read & Write` permission, install it on your repo, then add two repository secrets:

| Secret | Value |
|---|---|
| `PGREVIEWER_APP_ID` | Numeric App ID from the app's settings page |
| `PGREVIEWER_APP_PRIVATE_KEY` | Full contents of the downloaded `.pem` file |

Replace the `permissions` block and add a token generation step:

```yaml
permissions:
  contents: read   # pull-requests/checks handled by the app token

jobs:
  review:
    steps:
      - uses: actions/create-github-app-token@v1
        id: app-token
        with:
          app-id: ${{ secrets.PGREVIEWER_APP_ID }}
          private-key: ${{ secrets.PGREVIEWER_APP_PRIVATE_KEY }}

      - uses: actions/checkout@v4
        # ... rest of steps unchanged, replace secrets.GITHUB_TOKEN with:
        #     steps.app-token.outputs.token
```

---

## What pgReviewer catches

| Category | Detector | Severity |
|---|---|---|
| **EXPLAIN analysis** | `sequential_scan` — seq scan on tables >10K rows | WARNING / CRITICAL |
| | `missing_index_on_filter` — filter without supporting index | WARNING |
| | `nested_loop_large_outer` — nested loop with large outer relation | WARNING / CRITICAL |
| | `high_cost` — query cost exceeds threshold | WARNING / CRITICAL |
| | `sort_without_index` — sort that could use an index | WARNING |
| | `cartesian_join` — join without condition | **always CRITICAL** |
| **Migration safety** | `add_foreign_key_without_index` — FK without supporting index | **always CRITICAL** |
| | `add_not_null_without_default` — NOT NULL addition on existing table | CRITICAL |
| | `add_column_with_default` — non-trivial default rewrites table pre-PG11 | WARNING |
| | `create_index_not_concurrently` — write lock on large tables | WARNING |
| | `alter_column_type` — column type change rewrites the table | CRITICAL |
| | `destructive_ddl` — DROP TABLE / DROP COLUMN / TRUNCATE | WARNING |
| | `large_table_ddl` — any DDL above row count threshold | WARNING |
| | `drop_column_referenced` — column removed still used in queries | CRITICAL |
| **Code patterns** | `query_in_loop` — N+1: DB call inside a loop | WARNING / CRITICAL |
| | cross-file N+1 — loop in service calls query in repository | WARNING |
| | SQLAlchemy model diff — detects removed indexes, missing FK indexes | WARNING / CRITICAL |

All findings include a copy-ready fix — `CREATE INDEX CONCURRENTLY …`, a two-phase migration pattern, or a batch query alternative.

---

## How it works

<p align="center">
  <img src="docs/assets/pipeline.svg" alt="Analysis Pipeline" width="800"/>
</p>

pgReviewer runs `EXPLAIN (FORMAT JSON)` against your staging database — never `EXPLAIN ANALYZE`, never modifying your data. Index suggestions are validated with [HypoPG](https://hypopg.readthedocs.io/): a hypothetical index is created in a read-only transaction, `EXPLAIN` is re-run, the cost reduction is measured, and the transaction is rolled back. Only suggestions above **30% improvement** (configurable) make it into the report.

For complex plans (multi-join, CTEs, subqueries), an optional LLM step interprets the bottleneck. It degrades gracefully — algorithmic analysis always runs regardless of LLM availability.

---

## Local usage

```bash
pip install pgreviewer   # or: uv add pgreviewer

export DATABASE_URL=postgresql://user:pass@localhost:5432/mydb

pgr check "SELECT * FROM orders WHERE user_id = 42"   # single query
pgr diff --git-ref HEAD~1                              # last commit
pgr diff --staged                                      # pre-commit hook
pgr diff --git-ref main --ci                           # CI mode, exits 1 on CRITICAL
```

---

## Configuration

`.pgreviewer.yml` in your project root:

```yaml
rules:
  cartesian_join:
    enabled: false          # silence a detector
  sequential_scan:
    severity: critical      # override severity

thresholds:
  seq_scan_rows: 5000
  hypopg_min_improvement: 0.20

ignore:
  tables: [django_migrations, alembic_version]
  files: ["tests/fixtures/**"]
```

Full reference: [docs/configuration.md](docs/configuration.md)

---

## Postgres MCP Pro — better indexes

pgReviewer integrates with [Postgres MCP Pro](https://github.com/crystaldba/postgres-mcp),
an open-source MCP server for PostgreSQL. When a server is available, the
`hybrid` backend replaces the built-in index suggester with MCP Pro's
`analyze_query_indexes` — a workload-aware engine that batches up to 10 queries
per call, deduplicates overlapping candidates, and produces a consolidated
recommendation set instead of per-query hints.

```bash
# Start MCP Pro alongside your database
docker run -d --name pgr-mcp \
  -e DATABASE_URL=$DATABASE_URL \
  -p 8000:8000 crystaldba/postgres-mcp:latest

# Tell pgReviewer to use it
export BACKEND=hybrid
export MCP_SERVER_URL=http://localhost:8000/sse
```

If the MCP server is unreachable, pgReviewer falls back to the local engine
automatically — no configuration change required. See
[docs/mcp-integration.md](docs/mcp-integration.md) for the full CI setup,
GitHub Actions example, and fallback behaviour.

---

## Documentation

| | |
|---|---|
| [Getting Started](docs/getting-started.md) | Installation, Docker setup, first analysis |
| [CI Database Setup](docs/ci-database-setup.md) | Staging DB connection patterns for CI |
| [Configuration](docs/configuration.md) | All settings, thresholds, and environment variables |
| [Issue Detectors](docs/detectors.md) | Detector reference and custom detector API |
| [Analysis Pipeline](docs/analysis.md) | How the multi-stage engine works |
| [Postgres MCP Pro Integration](docs/mcp-integration.md) | Hybrid backend, better index recommendations |

---

## Development

```bash
uv sync
uv run pytest tests/unit -v        # unit tests (no database required)
uv run pytest -m integration       # integration tests (requires DATABASE_URL)
uv run ruff check . && uv run ruff format .
```

---

## License

MIT
