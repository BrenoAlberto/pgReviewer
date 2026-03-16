# Debug Store

The Debug Store persists intermediate data from every analysis run, making each decision auditable and reproducible.

## Storage Structure

```
~/.pgreviewer/debug/
└── 2025-01-15/
    ├── 20250115-143022-a1b2c3/
    │   ├── EXPLAIN_PLAN.json
    │   ├── HYPOPG_VALIDATION.json
    │   └── RECOMMENDATIONS.json
    └── 20250115-150145-d4e5f6/
        ├── EXPLAIN_PLAN.json
        └── RECOMMENDATIONS.json
```

| Path Component | Description |
|---------------|-------------|
| `~/.pgreviewer/debug/` | Root directory (configurable via `DEBUG_STORE_PATH`) |
| `YYYY-MM-DD/` | Date partition for easy cleanup |
| `YYYYMMDD-HHMMSS-{uuid}/` | Unique run identifier |
| `{CATEGORY}.json` | Artifact file |

## Artifact Categories

| Category | When Created | Contents |
|----------|-------------|----------|
| `EXPLAIN_PLAN` | Every run | Raw PostgreSQL execution plan JSON |
| `HYPOPG_VALIDATION` | When indexes are suggested | Before/after costs, plan shapes, improvement percentages |
| `LLM_PROMPT` | Future (LLM integration) | Exact prompt sent to the LLM |
| `LLM_RESPONSE` | Future (LLM integration) | Raw LLM response JSON |
| `RECOMMENDATIONS` | When issues are found | Final processed index recommendations |

## CLI Usage

### List recent runs

```bash
$ pgr debug list

Date         Run ID                         Query Snippet
──────────────────────────────────────────────────────────────
2025-01-15   20250115-143022-a1b2c3         SELECT * FROM orders WHERE ...
2025-01-15   20250115-150145-d4e5f6         SELECT o.*, u.name FROM ...
```

### Inspect a run

```bash
$ pgr debug show 20250115-143022-a1b2c3

--- EXPLAIN_PLAN ---
{
  "Plan": {
    "Node Type": "Seq Scan",
    "Relation Name": "orders",
    ...
  }
}

--- HYPOPG_VALIDATION ---
{
  "candidates": [
    {
      "index": "CREATE INDEX ON orders (user_id)",
      "cost_before": 4521.00,
      "cost_after": 8.00,
      "improvement_pct": 0.998
    }
  ]
}
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DEBUG_STORE_PATH` | `~/.pgreviewer/debug` | Root directory for artifacts |

To change the storage location:

```bash
DEBUG_STORE_PATH=/tmp/pgreviewer-debug
```

## Cleanup

Artifacts are organized by date, making cleanup straightforward:

```bash
# Remove runs older than 30 days
find ~/.pgreviewer/debug -maxdepth 1 -type d -mtime +30 -exec rm -rf {} +
```
