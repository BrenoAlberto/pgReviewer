# Getting Started

This guide walks you through installing pgReviewer, setting up a database, and running your first query analysis.

## Prerequisites

- **Python 3.12+**
- **Docker & Docker Compose** (for the development database)
- **[uv](https://docs.astral.sh/uv/)** (recommended) or pip

## Installation

### From source

```bash
git clone https://github.com/BrenoAlberto/pgReviewer.git
cd pgReviewer
uv sync
```

This installs pgReviewer and registers the `pgr` CLI command.

## Database Setup

pgReviewer needs a PostgreSQL instance with the [HypoPG](https://hypopg.readthedocs.io/) extension to validate index recommendations.

### Start PostgreSQL

```bash
docker compose up -d
```

This launches PostgreSQL 16 with HypoPG pre-installed. The init script at `db/init/00_extensions.sql` runs `CREATE EXTENSION IF NOT EXISTS hypopg;` automatically.

### Configure the connection

```bash
cp .env.example .env
```

The default `.env` connects to the Docker database:

```
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/pgreviewer
```

### Seed test data

```bash
pgr db seed
```

This populates the database with realistic data across `users`, `orders`, and `products` tables (100K+ rows with power-law distributions). Realistic data is important because PostgreSQL's `EXPLAIN` cost estimates depend on accurate table statistics.

## Your First Analysis

### Analyze a single query

```bash
pgr check "SELECT * FROM orders WHERE user_id = 42"
```

If there's no index on `user_id`, pgReviewer will:

1. Run `EXPLAIN` to get the query plan
2. Detect the sequential scan on a large table
3. Suggest a btree index on `orders(user_id)`
4. Create a hypothetical index with HypoPG
5. Re-run `EXPLAIN` to measure the cost improvement
6. Report the validated recommendation with before/after costs

### Analyze from a file

```bash
pgr check -f path/to/query.sql
```

### Get JSON output

```bash
pgr check "SELECT * FROM orders WHERE user_id = 42" --json
```

The JSON output includes all issues, recommendations, and cost comparisons — useful for CI pipelines or further processing.

## Understanding the Output

```
──────────────────── pgReviewer Analysis ────────────────────
Query:   SELECT * FROM orders WHERE user_id = 42
Overall: 🟡 WARNING

┌──────────┬─────────────────────┬──────────────────────────────┬────────────────────────┐
│ Severity │ Detector            │ Description                  │ Suggested Action       │
├──────────┼─────────────────────┼──────────────────────────────┼────────────────────────┤
│ 🟡 WARN  │ sequential_scan     │ Seq Scan on orders (150K)    │ Add index on user_id   │
└──────────┴─────────────────────┴──────────────────────────────┴────────────────────────┘

──────────────────── Recommended Indexes ────────────────────
💡 Suggested index (HypoPG validated ✓)
   CREATE INDEX CONCURRENTLY idx_orders_user_id ON orders (user_id);
   Cost: 4521.00 → 8.00  (improvement: 99.8%)
```

- **Severity levels**: `CRITICAL` (must fix), `WARNING` (should fix), `INFO` (informational)
- **Validated indexes**: Tested with HypoPG, showing actual cost reduction
- **CREATE INDEX CONCURRENTLY**: Ready to copy and run — won't lock your table

## Debugging Analysis Runs

Every analysis run is persisted for inspection:

```bash
# List recent runs
pgr debug list

# Inspect a specific run
pgr debug show <run_id>
```

This shows the raw EXPLAIN plan, HypoPG validation results, and any other artifacts from the analysis.

## Next Steps

- [Analysis Pipeline](analysis.md) — How the analysis engine works in detail
- [Configuration](configuration.md) — Tuning thresholds and disabling detectors
- [Detectors](detectors.md) — All built-in detectors and how to write your own
