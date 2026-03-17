# Configuration

pgReviewer is configured in two layers:

1. **`.pgreviewer.yml`** — project-level rules, thresholds, and suppression (committed to the repo)
2. **Environment variables / `.env`** — secrets and infrastructure settings (not committed)

---

## .pgreviewer.yml

Place this file in the project root (or the directory you run `pgr` from).

```yaml
rules:
  # Override severity or disable any detector by name
  sequential_scan:
    severity: critical      # promote to critical
  large_table_ddl:
    enabled: false          # silence entirely
  high_cost:
    severity: warning       # demote from critical

thresholds:
  seq_scan_rows: 5000               # flag seq scans above this row estimate
  large_table_ddl_rows: 500000      # flag DDL above this table size
  high_cost: 5000.0                 # plan cost threshold for WARNING
  hypopg_min_improvement: 0.20      # minimum improvement ratio to recommend an index

ignore:
  tables:
    - django_migrations
    - alembic_version
  files:
    - "tests/fixtures/**"
    - "seeds/**"
  rules:
    - drop_column_referenced   # suppress a rule for the whole project
```

Validate your config:

```bash
pgr config validate
pgr config init    # scaffold a .pgreviewer.yml with all defaults
```

---

## Environment variables

### Database

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | *required* | PostgreSQL connection string |
| `READ_ONLY` | `true` | Restricts to read-only + HypoPG (always rolled back) |

### Backend

| Variable | Default | Description |
|---|---|---|
| `BACKEND` | `local` | `local`, `mcp`, or `hybrid` |
| `MCP_SERVER_URL` | `http://localhost:8000/sse` | Postgres MCP Pro endpoint |
| `MCP_TIMEOUT_SECONDS` | `30` | Timeout for MCP calls |

### Detection thresholds

These can also be set in `.pgreviewer.yml` under `thresholds:`.

| Variable | Default | Description |
|---|---|---|
| `SEQ_SCAN_ROW_THRESHOLD` | `10000` | Row estimate before a seq scan is flagged WARNING |
| `SEQ_SCAN_CRITICAL_THRESHOLD` | `1000000` | Row estimate for CRITICAL seq scan |
| `NESTED_LOOP_OUTER_THRESHOLD` | `1000` | Outer-relation rows before nested loop is flagged |
| `HIGH_COST_THRESHOLD` | `10000.0` | Plan cost for WARNING |
| `HIGH_COST_CRITICAL_THRESHOLD` | `100000.0` | Plan cost for CRITICAL |
| `HYPOPG_MIN_IMPROVEMENT` | `0.30` | Minimum cost improvement ratio to recommend an index |
| `LARGE_TABLE_DDL_THRESHOLD` | `100000` | Rows above which any DDL is flagged |

### Detectors and ignore lists

| Variable | Default | Description |
|---|---|---|
| `DISABLED_DETECTORS` | `[]` | Detector names to skip |
| `IGNORE_TABLES` | `[]` | Tables excluded from all detectors |
| `IGNORE_PATHS` | `[]` | File glob patterns excluded from diff analysis |
| `TRIGGER_PATHS` | see below | Glob patterns that trigger `pgr diff` analysis |
| `QUERY_METHODS` | `["execute","fetch",…]` | Method names treated as DB calls by code-pattern detectors |

Default `TRIGGER_PATHS`:

```
**.sql
**.py
**/migrations/**
**/models.py
**/models/**/*.py
```

### LLM

| Variable | Default | Description |
|---|---|---|
| `LLM_API_KEY` | `None` | Anthropic API key — enables LLM-assisted analysis |
| `LLM_MONTHLY_BUDGET_USD` | `10.0` | Monthly cap; LLM calls are skipped when exceeded |
| `LLM_COST_PER_TOKEN` | `0.00001` | Cost estimate used for pre-call budget checks |

Without `LLM_API_KEY`, pgReviewer runs full algorithmic analysis — LLM is strictly optional.

### Storage

| Variable | Default | Description |
|---|---|---|
| `DEBUG_STORE_PATH` | `~/.pgreviewer/debug` | EXPLAIN plans and analysis artifacts |
| `COST_STORE_PATH` | `~/.pgreviewer/costs` | LLM spend logs |
