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

---

## Usage

pgReviewer posts directly to your PRs — a summary comment with all findings, plus inline review comments with copy-ready fixes at the exact line that needs attention.

| PR summary | Inline fix suggestion |
|---|---|
| ![PR Warning Summary](docs/assets/pr_warn_summary_comment.png) | ![Index Not Concurrently Warning](docs/assets/pr_idx_concurrently_comment.png) |

![Query in Loop (N+1) Detection](docs/assets/pr_query_loop_comment.png)

---

## Add to your repo

**Step 1 — Install the pgreviewer-ci GitHub App**

[**Install pgreviewer-ci →**](https://github.com/apps/pgreviewer-ci)

This gives pgReviewer permission to post comments and reviews to your PRs.

**Step 2 — Create `.github/workflows/pgreviewer.yml`**

```yaml
name: pgReviewer

on:
  issue_comment:
    types: [created]
  pull_request:
    types: [opened]

permissions:
  contents: read
  issues: write
  pull-requests: write
  checks: write
  id-token: write   # required for pgreviewer-ci[bot] to post comments

jobs:
  pgreviewer:
    uses: BrenoAlberto/pgReviewer/.github/workflows/review.yml@main
    secrets: inherit          # forwards ANTHROPIC_API_KEY / OPENAI_API_KEY / GEMINI_API_KEY
    with:
      database-url: postgresql://user:pass@127.0.0.1:5432/mydb
      # run-migrations: true   # run alembic upgrade head before analysis
```

Add at least one LLM secret (**Settings → Secrets → Actions**) to enable AI-assisted insights. Supports `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, and `GEMINI_API_KEY`. See [docs/github-actions.md](docs/github-actions.md#llm-provider-setup) for provider details.

That's it. All analysis logic and inline suggestion diffs live in pgReviewer and update automatically.

**How it works:**
- When a PR opens → `pgreviewer-ci[bot]` posts a welcome comment with the `/pgr review` command and available LLM options.
- When someone comments `/pgr review` → 👀 appears immediately, analysis runs, then 👀 is replaced with 🚀 (pass) or 😕 (criticals found). Results are posted as a summary comment + inline suggestion diffs.
- Pass `--model gpt-4o` or `--model gemini-2.0-flash` in the comment to switch providers on the fly.

For staging database connection patterns (Docker sidecar, Cloud SQL Proxy, direct) see [docs/ci-database-setup.md](docs/ci-database-setup.md). For advanced workflow options see [docs/github-actions.md](docs/github-actions.md).

## What pgReviewer catches

- **EXPLAIN analysis** — sequential scans on large tables, missing indexes, nested loops, high-cost plans, cartesian joins
- **Migration safety** — FK without index, NOT NULL on existing tables, non-concurrent index creation, destructive DDL, column type changes, dropped columns still referenced in queries
- **Code patterns** — N+1 query-in-loop, cross-file N+1, SQLAlchemy model diff (removed indexes, missing FK indexes)

All findings include a copy-ready fix. Full detector reference: [docs/detectors.md](docs/detectors.md)

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

## Documentation

| | |
|---|---|
| [Getting Started](docs/getting-started.md) | Installation, Docker setup, first analysis |
| [CI Database Setup](docs/ci-database-setup.md) | Staging DB connection patterns for CI |
| [GitHub Actions](docs/github-actions.md) | Always-comment mode and advanced workflow options |
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
