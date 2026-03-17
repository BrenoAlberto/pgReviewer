# Configuration

pgReviewer is configured through environment variables or a `.env` file in the project root. All settings use [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) for validation and type coercion.

## Quick Setup

```bash
cp .env.example .env
# Edit .env with your database connection
```

## Reference

### Database

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `DATABASE_URL` | `str` | *required* | PostgreSQL connection string. Example: `postgresql://user:pass@localhost:5432/dbname` |
| `READ_ONLY` | `bool` | `True` | Safety mode. When enabled, only read operations and HypoPG writes (always rolled back) are allowed |

### Detection Thresholds

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SEQ_SCAN_ROW_THRESHOLD` | `int` | `10000` | Minimum row estimate before a sequential scan triggers a WARNING |
| `SEQ_SCAN_CRITICAL_THRESHOLD` | `int` | `1000000` | Row estimate threshold for CRITICAL sequential scan |
| `NESTED_LOOP_OUTER_THRESHOLD` | `int` | `1000` | Minimum outer-relation rows before nested loop is flagged |
| `HIGH_COST_THRESHOLD` | `float` | `10000.0` | Plan cost threshold for WARNING |
| `HIGH_COST_CRITICAL_THRESHOLD` | `float` | `100000.0` | Plan cost threshold for CRITICAL |
| `HYPOPG_MIN_IMPROVEMENT` | `float` | `0.30` | Minimum cost improvement ratio (0.0–1.0) required to recommend an index |

### Detectors & Tables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `DISABLED_DETECTORS` | `list[str]` | `[]` | Detector names to skip during analysis |
| `QUERY_METHODS` | `list[str]` | `["execute","fetch","fetchrow","fetchval","fetchone","fetchall"]` | Method names treated as DB query calls by code-pattern detectors |
| `IGNORE_TABLES` | `list[str]` | `[]` | Tables to exclude from all detectors |

### LLM Budget (Infrastructure)

These settings are in place for future LLM integration. No LLM calls are made in the current version.

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `LLM_API_KEY` | `str \| None` | `None` | API key for LLM-powered analysis |
| `LLM_MONTHLY_BUDGET_USD` | `float` | `10.0` | Monthly budget cap for LLM calls |
| `LLM_COST_PER_TOKEN` | `float` | `0.00001` | Estimated cost per token for budget pre-checks |
| `LLM_CATEGORY_LIMITS` | `dict` | `{"review": 0.5, "summary": 0.3, "general": 0.2}` | Budget split by category (fractions of total) |

### Storage

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `DEBUG_STORE_PATH` | `Path` | `~/.pgreviewer/debug` | Directory for analysis artifacts (EXPLAIN plans, validations) |
| `COST_STORE_PATH` | `Path` | `~/.pgreviewer/costs` | Directory for LLM cost tracking logs |

## Docker Compose Variables

These variables are used by `docker-compose.yml`:

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_PASSWORD` | `postgres` | Password for the PostgreSQL container |
| `POSTGRES_DB` | `pgreviewer` | Database name for the container |

## Example `.env`

```bash
# Database
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/pgreviewer
READ_ONLY=True

# Tuning
SEQ_SCAN_ROW_THRESHOLD=10000
HIGH_COST_THRESHOLD=10000.0
HYPOPG_MIN_IMPROVEMENT=0.30

# Disable specific detectors
DISABLED_DETECTORS=[]

# Extend DB query call method names for code-pattern checks
QUERY_METHODS=["execute","fetch","fetchrow","fetchval","my_custom_db_method"]

# Ignore specific tables
IGNORE_TABLES=[]

# Storage
DEBUG_STORE_PATH=~/.pgreviewer/debug
COST_STORE_PATH=~/.pgreviewer/costs
```
