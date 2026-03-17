<p align="center">
  <img src="docs/assets/logo.svg" alt="pgReviewer" width="120" />
</p>

<h1 align="center">pgReviewer</h1>

<p align="center">
  <strong>Catch slow PostgreSQL queries before they hit production.</strong>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> &middot;
  <a href="#how-it-works">How It Works</a> &middot;
  <a href="#cli-reference">CLI Reference</a> &middot;
  <a href="#github-action-triggering">GitHub Action Triggering</a> &middot;
  <a href="docs/">Documentation</a> &middot;
  <a href="#roadmap">Roadmap</a>
</p>

---

pgReviewer is a command-line tool that analyzes SQL queries against a real PostgreSQL instance, detects performance issues using `EXPLAIN` plans, and suggests validated index improvements using [HypoPG](https://hypopg.readthedocs.io/) — all without modifying your database.

```
$ pgr check "SELECT * FROM orders WHERE user_id = 42"

──────────────────── pgReviewer Analysis ────────────────────
Query:   SELECT * FROM orders WHERE user_id = 42
Overall: 🟡 WARNING

┌──────────┬─────────────────────────────┬───────────────────────────────┬────────────────────────────┐
│ Severity │ Detector                    │ Description                   │ Suggested Action           │
├──────────┼─────────────────────────────┼───────────────────────────────┼────────────────────────────┤
│ 🟡 WARN  │ sequential_scan             │ Seq Scan on orders (150K rows)│ Add index on user_id       │
│ 🟡 WARN  │ missing_index_on_filter     │ Filter on user_id, no index  │ Create btree index         │
└──────────┴─────────────────────────────┴───────────────────────────────┴────────────────────────────┘

──────────────────── Recommended Indexes ────────────────────
💡 Suggested index (HypoPG validated ✓)
   CREATE INDEX CONCURRENTLY idx_orders_user_id ON orders (user_id);
   Cost: 4521.00 → 8.00  (improvement: 99.8%)
```

## Features

- **EXPLAIN-based analysis** — Runs `EXPLAIN` against your database to get real cost estimates, not guesses
- **6 built-in detectors** — Sequential scans, missing indexes, nested loops, cartesian joins, high-cost queries, unsupported sorts
- **HypoPG validation** — Every index suggestion is tested with a hypothetical index to prove it actually helps
- **Zero side effects** — Read-only by default. HypoPG operations run in always-rollback transactions
- **Pluggable architecture** — Add custom detectors by dropping a Python file into the detectors directory
- **JSON output** — Machine-readable output with `--json` for CI pipelines

## Quick Start

### 1. Copy the workflow into your repository

Create `.github/workflows/pgreviewer.yml` using the copy-paste-ready example:
[`docs/example-workflow.yml`](docs/example-workflow.yml).

### 2. Add required GitHub Actions secrets

In your repository, go to **Settings → Secrets and variables → Actions** and add:

- `PGREVIEWER_DB_URL` (required) — PostgreSQL connection string used for analysis.
- `ANTHROPIC_API_KEY` (optional) — enables LLM-assisted analysis; without it, pgReviewer still runs with algorithmic analysis.
- `GITHUB_TOKEN` (auto-provided by GitHub Actions; no manual secret creation needed).

### 3. Open a pull request that changes SQL-related files

On the next PR touching SQL, migrations, or models paths, pgReviewer runs and posts results to the pull request.

## How It Works

<p align="center">
  <img src="docs/assets/pipeline.svg" alt="Analysis Pipeline" width="700" />
</p>

pgReviewer runs a multi-stage analysis pipeline:

1. **EXPLAIN** — Executes `EXPLAIN (FORMAT JSON, COSTS, VERBOSE)` against your database (never `ANALYZE` — no side effects)
2. **Parse** — Converts the JSON plan into a typed tree structure for traversal
3. **Collect Schema** — Gathers table stats, existing indexes, and column statistics from PostgreSQL system catalogs
4. **Detect Issues** — Runs all enabled detectors against the plan and schema
5. **Suggest Indexes** — Generates index candidates based on detected issues (equality filters, sort columns, range predicates)
6. **Validate with HypoPG** — Creates hypothetical indexes, re-runs `EXPLAIN`, measures actual cost reduction
7. **Report** — Outputs results as a rich terminal report or JSON

Only indexes that achieve at least **30% cost improvement** (configurable) are recommended.

## CLI Reference

| Command | Description |
|---------|-------------|
| `pgr check "<sql>"` | Analyze a SQL query for performance issues |
| `pgr check -f query.sql` | Analyze SQL from a file |
| `pgr check "<sql>" --json` | Output results as JSON |
| `pgr check "<sql>" --verbose` | Show full EXPLAIN JSON and detailed interpretation |
| `pgr check "<sql>" --no-color` | Disable ANSI colors for plain text output |
| `pgr check-models --path src/` | Perform static checks on SQLAlchemy models |
| `pgr check-models --path src/ --fix` | Suggest missing index definitions |
| `pgr workload --top 5` | Analyze the top slow workload queries and suggest indexes |
| `pgr workload --min-calls 100 --export markdown` | Filter noisy workload queries and output a Markdown table |
| `pgr catalog build` | Rebuild query-function catalog from Python files |
| `pgr catalog show` | Show cataloged query functions |
| `pgr backend status` | Show configured backend and connectivity status |
| `pgr version` | Print installed version |
| `pgr cost` | Show LLM spend breakdown (infrastructure ready) |
| `pgr db seed` | Seed database with realistic test data |
| `pgr debug list` | List recent analysis runs |
| `pgr debug show <run_id>` | Inspect artifacts from a specific run |

## GitHub Action Triggering

pgreviewer only runs when your PR touches SQL-related files.

Use the example workflow in
[`docs/example-workflow.yml`](docs/example-workflow.yml) and
keep the `pull_request.paths` filter scoped to SQL/migration/model files:

```yaml
on:
  pull_request:
    paths:
      - '**.sql'
      - '**/migrations/**'
      - '**/models/**'
```

If your repository layout differs, pass the optional `trigger_paths` action input
as a comma-separated glob list.

For connecting pgReviewer to a staging database in CI — including minimum required permissions, Docker sidecar setup, and Cloud SQL Proxy configuration — see the **[CI Database Setup guide](docs/ci-database-setup.md)**.

## Issue Detectors

pgReviewer ships with 6 detectors that analyze `EXPLAIN` plans:

| Detector | Severity | What it catches |
|----------|----------|-----------------|
| `sequential_scan` | WARNING / CRITICAL | Seq Scan on tables with >10K rows (configurable) |
| `missing_index_on_filter` | WARNING | Filter conditions without a supporting index |
| `nested_loop_large_outer` | WARNING / CRITICAL | Nested loop joins with >1K outer rows |
| `high_cost` | WARNING / CRITICAL | Queries exceeding cost threshold (default: 10K) |
| `sort_without_index` | WARNING | Sort operations that could use an index |
| `cartesian_join` | CRITICAL | Joins without conditions (cross products) |

### Adding a Custom Detector

Create a file in `pgreviewer/analysis/issue_detectors/`:

```python
from pgreviewer.analysis.issue_detectors import BaseDetector
from pgreviewer.core.models import ExplainPlan, Issue, Severity, SchemaInfo

class MyDetector(BaseDetector):
    @property
    def name(self) -> str:
        return "my_custom_check"

    def detect(self, plan: ExplainPlan, schema: SchemaInfo) -> list[Issue]:
        issues = []
        # Your detection logic here
        return issues
```

It will be automatically discovered and run during analysis. To disable it, add `"my_custom_check"` to `DISABLED_DETECTORS` in your `.env`.

## Configuration

All settings are managed through environment variables or a `.env` file. See [`.env.example`](.env.example) for the full list.

### Deployment modes

| Mode | EXPLAIN | Index Rec | Schema | Requires |
|------|---------|-----------|--------|----------|
| `local` | asyncpg | HypoPG | pg_catalog | DB connection |
| `mcp` | MCP Pro | MCP Pro | MCP Pro | MCP server |
| `hybrid` | asyncpg | MCP Pro | MCP Pro | Both |

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | — | PostgreSQL connection string (required) |
| `SEQ_SCAN_ROW_THRESHOLD` | `10000` | Min rows before a seq scan is flagged |
| `HIGH_COST_THRESHOLD` | `10000.0` | Query cost threshold for warnings |
| `HYPOPG_MIN_IMPROVEMENT` | `0.30` | Min cost improvement to recommend an index (30%) |
| `DISABLED_DETECTORS` | `[]` | Detector names to skip |
| `IGNORE_TABLES` | `[]` | Tables to exclude from analysis |
| `DEBUG_STORE_PATH` | `~/.pgreviewer/debug` | Where to store analysis artifacts |
| `READ_ONLY` | `True` | Safety mode — only HypoPG writes (always rolled back) |

## Project Structure

```
pgreviewer/
├── analysis/
│   ├── explain_runner.py        # EXPLAIN plan execution
│   ├── plan_parser.py           # JSON → typed Pydantic tree
│   ├── schema_collector.py      # Table stats from pg_class/pg_stats
│   ├── index_suggester.py       # Algorithmic index candidate generation
│   ├── index_generator.py       # CREATE INDEX statement generation
│   ├── hypopg_validator.py      # Hypothetical index validation
│   └── issue_detectors/         # Pluggable detector modules
│       ├── sequential_scan.py
│       ├── missing_index_on_filter.py
│       ├── nested_loop.py
│       ├── high_cost.py
│       ├── sort_without_index.py
│       └── cartesian_join.py
├── cli/
│   ├── main.py                  # CLI entry point (Typer)
│   └── commands/
│       └── check.py             # pgr check implementation
├── core/
│   ├── models.py                # Pydantic models & dataclasses
│   └── severity.py              # Risk classification logic
├── db/
│   └── pool.py                  # asyncpg pool, read/write sessions
├── infra/
│   ├── debug_store.py           # Artifact persistence (JSON)
│   └── cost_guardrail.py        # LLM budget enforcement
├── config.py                    # pydantic-settings configuration
└── exceptions.py                # Custom exception hierarchy
```

## Development

### Running Tests

```bash
# Unit tests (no database needed)
uv run pytest tests/ -v

# With coverage
uv run pytest tests/ --cov=pgreviewer --cov-report=term-missing

# Integration tests (live PostgreSQL required)
uv run pytest -m integration

# Skip integration tests explicitly
SKIP_INTEGRATION_TESTS=1 uv run pytest tests/
```

### Running MCP Integration Tests

MCP integration tests require a running **Postgres MCP Pro** instance.
They are never included in the standard test run and must be explicitly
opted into with `-m mcp`.

#### 1. Start Postgres MCP Pro

Install [postgres-mcp](https://github.com/crystaldba/postgres-mcp) and
point it at your local database:

```bash
uvx postgres-mcp \
  --connection-string "postgresql://postgres:postgres@127.0.0.1:5432/pgreviewer" \
  --transport sse \
  --sse-port 8000
```

The SSE endpoint will be available at `http://localhost:8000/sse` (the default
`MCP_SERVER_URL` used by the tests).

> **Note:** Use `127.0.0.1` instead of `localhost` in the connection string on
> Linux to force a TCP connection; `localhost` may resolve to a Unix socket.

#### 2. Seed the database (if not already done)

```bash
pgr db seed
```

#### 3. Run only the MCP tests

```bash
uv run pytest -m mcp -v
```

#### 4. Skip MCP tests in CI

```bash
uv run pytest -m "not mcp"
# or:
SKIP_MCP_TESTS=1 uv run pytest tests/
```

#### Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `MCP_SERVER_URL` | `http://localhost:8000/sse` | MCP Pro endpoint used by the tests |
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5432/pgreviewer` | PostgreSQL connection for fixture setup |

### Code Quality

```bash
# Lint
uv run ruff check .

# Format
uv run ruff format .
```

### Pre-commit Hooks

```bash
uv run pre-commit install
```

## Roadmap

pgReviewer is under active development. Here's what's planned:

- [x] **EXPLAIN-based analysis** — Query analysis with issue detection and severity classification
- [x] **HypoPG validation** — Hypothetical index testing with before/after cost comparison
- [x] **Index generation** — Ready-to-run `CREATE INDEX CONCURRENTLY` statements
- [ ] **Git diff analysis** — Extract SQL from diffs and migration files *(in progress)*
- [ ] **Tree-sitter parsing** — Multi-language SQL extraction from Python source code
- [ ] **Migration safety** — Detect dangerous DDL patterns (table locks, rewrites)
- [ ] **N+1 detection** — Find queries inside loops via static analysis
- [ ] **LLM integration** — AI-assisted analysis for complex query plans
- [ ] **GitHub Action** — Automatic PR comments with performance analysis
- [ ] **MCP server** — Expose analysis tools for IDE integration

## License

MIT
