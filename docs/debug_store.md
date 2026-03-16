# Debug Store

The `DebugStore` is responsible for persisting intermediate data from the analysis pipeline. This ensures that every decision made by the LLM can be audited and reproduced.

## Storage Structure

Files are stored in the following hierarchy:
`{DEBUG_STORE_PATH}/{YYYY-MM-DD}/{RUN_ID}/{CATEGORY}.json`

- **DEBUG_STORE_PATH**: Configurable in `.env`, defaults to `~/.pgreviewer/debug`.
- **YYYY-MM-DD**: Partitioning by date for easy cleanup and organization.
- **RUN_ID**: Unique identifier for a single analysis run (`YYYYMMDD-HHMMSS-{short_uuid}`).
- **CATEGORY**: Type of artifact stored.

## Artifact Categories

| Category | Description |
|----------|-------------|
| `EXPLAIN_PLAN` | The raw PostgreSQL execution plan being analyzed. |
| `LLM_PROMPT` | The exact prompt sent to the LLM. |
| `LLM_RESPONSE` | The raw JSON response received from the LLM. |
| `RECOMMENDATIONS`| The final processed recommendations. |
| `HYPOPG_VALIDATION`| Detailed before/after costs and plan changes for index candidates. |

## CLI Usage

### Listing Runs
To see recent analysis runs:
```bash
pgr debug list
```

### Inspecting a Run
To see all artifacts for a specific run ID:
```bash
pgr debug show <run_id>
```
