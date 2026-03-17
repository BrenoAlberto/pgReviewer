# Postgres MCP Pro Integration

[Postgres MCP Pro](https://github.com/crystaldba/postgres-mcp) is an open-source
Model Context Protocol server for PostgreSQL. pgReviewer treats it as an optional
but powerful backend: when an MCP server is reachable, index recommendations improve
significantly; when it is not, pgReviewer falls back to its built-in engine without
any user action required.

---

## Why it matters

pgReviewer's built-in `local` engine runs `EXPLAIN` and validates index candidates
with HypoPG. Postgres MCP Pro's `analyze_query_indexes` tool goes further — it
models the full workload, deduplicates overlapping indexes across queries, and
produces a single consolidated recommendation set instead of per-query suggestions.
The difference is most visible in repos with dozens of related queries hitting the
same tables: `local` may suggest five separate indexes; MCP Pro might suggest two
composite indexes that cover all five.

Additional capabilities exposed over MCP:

| Tool | What pgReviewer uses it for |
|---|---|
| `analyze_query_indexes` | Batched index recommendations (up to 10 queries per call, deduplicated) |
| `execute_sql` | Running `EXPLAIN` when used in `mcp` mode |
| `get_object_details` | Richer schema metadata — column stats, FK graph, partition info |
| `get_top_queries` | pg_stat_statements workload — escalates severity for already-slow queries |

---

## Backend modes

<p align="center">
  <img src="assets/mcp-modes.svg" alt="Backend Modes" width="760"/>
</p>

### `local` (default)

Everything runs on a direct database connection: `EXPLAIN` via asyncpg, index
validation via HypoPG, schema via `pg_catalog`. No MCP server required. Works
in any CI environment where you can reach the database.

### `mcp`

All database access goes through the MCP server. Useful when the CI runner cannot
open a raw TCP connection to the database but the MCP server can (e.g., the MCP
server runs as a sidecar with network access that CI lacks). If the MCP server is
unavailable or returns an error, pgReviewer automatically falls back to `local` for
that operation.

### `hybrid` (recommended when MCP is available)

`EXPLAIN` runs locally (fast, full plan detail), while index recommendations and
schema metadata come from MCP Pro (superior quality). This is the best of both: the
low latency of a direct connection for plan capture and the algorithmic depth of MCP
Pro for suggestions.

---

## Setup

### 1. Start Postgres MCP Pro

```bash
docker run -d --name pgr-mcp \
  -e DATABASE_URL=postgresql://user:pass@host:5432/mydb \
  -p 8000:8000 crystaldba/postgres-mcp:latest
```

Or use the `docker-compose` approach in the
[Postgres MCP Pro docs](https://github.com/crystaldba/postgres-mcp).

### 2. Point pgReviewer at the server

```bash
export BACKEND=hybrid
export MCP_SERVER_URL=http://localhost:8000/sse
export DATABASE_URL=postgresql://user:pass@host:5432/mydb
```

Or in `.pgreviewer.yml`:

```yaml
backend:
  mode: hybrid
  mcp_server_url: http://localhost:8000/sse
```

### 3. Run as normal

```bash
pgr check "SELECT * FROM orders WHERE user_id = 42"
pgr diff --git-ref main
```

The report will include a `[mcp]` tag next to index recommendations sourced from
MCP Pro.

---

## In GitHub Actions

Add the MCP sidecar service and two extra environment variables:

```yaml
jobs:
  review:
    runs-on: ubuntu-latest
    services:
      pgr-db:
        image: ghcr.io/brenoalberto/pgreviewer-db:latest
        env:
          POSTGRES_USER: pgr
          POSTGRES_PASSWORD: pgr
          POSTGRES_DB: review_db
        ports: ["5432:5432"]
        options: --health-cmd pg_isready --health-interval 5s --health-retries 10

      pgr-mcp:
        image: crystaldba/postgres-mcp:latest
        env:
          DATABASE_URL: postgresql://pgr:pgr@pgr-db:5432/review_db
        ports: ["8000:8000"]

    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }

      - uses: astral-sh/setup-uv@v5
      - run: uv sync

      - name: Download PR diff
        run: gh pr diff ${{ github.event.pull_request.number }} > /tmp/pr.diff
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}

      - name: Analyze and post comment
        run: uv run pgr diff /tmp/pr.diff --json > /tmp/report.json || true
        env:
          DATABASE_URL: postgresql://pgr:pgr@127.0.0.1:5432/review_db
          BACKEND: hybrid
          MCP_SERVER_URL: http://localhost:8000/sse
```

---

## Auto-fallback behaviour

pgReviewer wraps every MCP call in a `try/except MCPError`. If the server is
unreachable, returns a timeout, or returns an unexpected response, the operation
is retried once and then falls back to the equivalent `local` implementation. The
analysis always completes — you will never get a hard failure because the MCP
server is down.

The fallback is logged at `WARNING` level so it is visible in CI output without
being noisy on every run.

---

## What changes in the report

When MCP Pro is active, findings sourced from it carry a `[mcp]` source tag in the
JSON output and in the GitHub PR comment. The `suggested_action` field contains the
deduplicated, workload-aware index recommendation rather than the per-query
candidate the local engine would have produced.

Workload escalation — when `get_top_queries` data is available — can raise a
finding from WARNING to CRITICAL if the affected query already appears in
`pg_stat_statements` with above-threshold total time.

---

## See also

- [Analysis Pipeline](analysis.md) — full pipeline overview and deployment mode comparison
- [CI Database Setup](ci-database-setup.md) — other CI connection patterns
- [Configuration](configuration.md) — all `BACKEND`, `MCP_SERVER_URL`, and `MCP_TIMEOUT_SECONDS` options
