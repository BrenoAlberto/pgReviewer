# pgReviewer

A tool for PostgreSQL query analysis and optimization.

## Development Environment

### Prerequisites
- Docker & Docker Compose
- `uv` (for Python dependency management)

### Starting the Database
To start the development database with HypoPG pre-installed:

```bash
docker compose up -d
```

### Stopping the Database
To stop the services:

```bash
docker compose down
```

To wipe the database and start fresh (removes volumes):

```bash
docker compose down -v
```

## DB Session Management

The project uses `asyncpg` for database interactions. Two session types are provided:

- `read_session()`: For read-only operations. It enforces `SET default_transaction_read_only = on`.
- `write_session()`: For HypoPG operations. It starts a transaction that is **always rolled back** on exit, ensuring no changes persist.

Example usage:

```python
from pgreviewer.db.pool import read_session, write_session

async with read_session() as conn:
    rows = await conn.fetch("SELECT * FROM users")

async with write_session() as conn:
    await conn.execute("SELECT hypopg_create_index('CREATE INDEX ON ...')")
    # Changes are rolled back automatically here
```

## Debug Store

pgReviewer persists query plans, LLM prompts, and recommendations for reproducible debugging. Artifacts are stored as JSON files partitioned by date.

### Commands

- `pgr check`: Run analysis and persist debug info.
- `pgr debug list`: List recent analysis runs.
- `pgr debug show <run_id>`: Show all artifacts for a specific run.

Artifacts are stored in `~/.pgreviewer/debug` by default (configurable via `DEBUG_STORE_PATH` in `.env`).
