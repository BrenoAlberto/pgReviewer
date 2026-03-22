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

Create `.github/workflows/pgreviewer.yml`:

```yaml
name: pgReviewer

on:
  pull_request:
    types: [opened, synchronize]

permissions:
  contents: read
  issues: write
  pull-requests: write

jobs:
  pgreviewer:
    uses: BrenoAlberto/pgReviewer/.github/workflows/review.yml@main
```

That's it — no secrets, no database required. pgReviewer runs **static analysis** automatically on every PR and posts findings as inline fix suggestions.

**Upgrade path:**

| Add this | To unlock |
|---|---|
| `id-token: write` permission + [pgreviewer-ci app](https://github.com/apps/pgreviewer-ci) | Comments posted as `pgreviewer-ci[bot]` |
| LLM secret (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, or `GEMINI_API_KEY`) | AI-enriched fix suggestions |
| `database-url` input + `issue_comment` trigger | On-demand `/pgr review` with EXPLAIN-based full analysis |

See [docs/github-actions.md](docs/github-actions.md) for the full tiered setup guide.

**How it works:**
- On every PR push → static analysis runs automatically. Findings are posted as a summary comment with inline one-click fix suggestions.
- On PR open → a welcome comment explains the `/pgr review` command (requires `database-url` to be configured).
- On `/pgr review` comment → 👀 appears, full EXPLAIN-based analysis runs, 👀 replaced with 🚀 or 😕. Results posted as summary + inline diffs.
- Pass `--model gpt-4o` or `--model gemini-2.0-flash` in the review comment to switch LLM providers on the fly.

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
| [GitHub Actions](docs/github-actions.md) | Tiered setup guide: static → LLM → full EXPLAIN |
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
