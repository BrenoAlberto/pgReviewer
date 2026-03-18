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

Every PR that touches SQL, migrations, or model files gets an automatic review comment and a ✅ / ❌ check status.

For staging database connection patterns (Docker sidecar, Cloud SQL Proxy, direct) see [docs/ci-database-setup.md](docs/ci-database-setup.md). For advanced workflow options see [docs/github-actions.md](docs/github-actions.md).

---

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
