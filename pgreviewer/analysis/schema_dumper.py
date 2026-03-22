"""Schema dump: pg_dump DDL + pg_stats metadata for offline analysis.

Produces a ``.pgreviewer/schema.sql`` file that combines:
1. ``pg_dump --schema-only`` output (DDL)
2. Per-table statistics appended as ``-- pgreviewer:stats`` JSON comments

This allows :mod:`pgreviewer` to run schema-aware static analysis
(severity escalation, index suggestions, row-count thresholds) without
requiring a live database connection in CI.
"""

from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING

from pgreviewer.exceptions import SchemaDumpError

if TYPE_CHECKING:
    from pathlib import Path

    import asyncpg

# ---------------------------------------------------------------------------
# pg_dump
# ---------------------------------------------------------------------------

_ALL_TABLES_STATS_QUERY = """
SELECT
    c.relname                       AS table_name,
    c.reltuples::bigint             AS row_estimate,
    pg_relation_size(c.oid)         AS size_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind = 'r';
"""

_ALL_INDEX_QUERY = """
SELECT
    ix.relname                      AS index_name,
    t.relname                       AS table_name,
    i.indisunique                   AS is_unique,
    pg_get_expr(i.indpred, i.indrelid)  AS predicate,
    am.amname                           AS index_type,
    array_agg(a.attname ORDER BY x.ordinality)
        FILTER (WHERE x.ordinality <= i.indnkeyatts) AS columns,
    array_agg(a.attname ORDER BY x.ordinality)
        FILTER (WHERE x.ordinality > i.indnkeyatts)  AS include_columns
FROM pg_index i
JOIN pg_class ix ON ix.oid = i.indexrelid
JOIN pg_class t  ON t.oid  = i.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_am am ON am.oid = ix.relam
CROSS JOIN LATERAL unnest(i.indkey)
    WITH ORDINALITY AS x(attnum, ordinality)
JOIN pg_attribute a
    ON a.attrelid = t.oid AND a.attnum = x.attnum
WHERE n.nspname = 'public'
  AND t.relkind = 'r'
  AND x.attnum > 0
GROUP BY
    ix.relname, t.relname, i.indisunique, i.indpred,
    i.indrelid, i.indnkeyatts, am.amname;
"""

_ALL_COLUMN_STATS_QUERY = """
SELECT
    s.tablename                     AS table_name,
    s.attname                       AS column_name,
    format_type(a.atttypid, a.atttypmod) AS column_type,
    s.null_frac                     AS null_fraction,
    s.n_distinct                    AS distinct_count
FROM pg_stats s
JOIN pg_class c
    ON c.relname = s.tablename
JOIN pg_namespace n
    ON n.oid = c.relnamespace AND n.nspname = s.schemaname
JOIN pg_attribute a
    ON a.attrelid = c.oid AND a.attname = s.attname
WHERE s.schemaname = 'public';
"""


def run_pg_dump(database_url: str) -> str:
    """Run ``pg_dump --schema-only`` and return the DDL as a string.

    Raises
    ------
    SchemaDumpError
        If ``pg_dump`` is not found or exits with an error.
    """
    try:
        result = subprocess.run(
            [
                "pg_dump",
                "--schema-only",
                "--no-owner",
                "--no-privileges",
                f"--dbname={database_url}",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        raise SchemaDumpError(
            "pg_dump not found in PATH. "
            "Install PostgreSQL client tools "
            "(e.g. `apt install postgresql-client`)."
        ) from None

    if result.returncode != 0:
        stderr = result.stderr.strip()
        raise SchemaDumpError(f"pg_dump failed (exit {result.returncode}): {stderr}")

    return result.stdout


# ---------------------------------------------------------------------------
# Stats collection
# ---------------------------------------------------------------------------


async def collect_all_stats(conn: asyncpg.Connection) -> dict[str, dict]:
    """Collect table, index, and column statistics for all public tables.

    Returns a dict keyed by table name:

    .. code-block:: python

        {
            "orders": {
                "row_estimate": 50000,
                "size_bytes": 4096000,
                "indexes": [...],
                "columns": [...]
            }
        }
    """
    tables: dict[str, dict] = {}

    # 1. Table-level stats
    rows = await conn.fetch(_ALL_TABLES_STATS_QUERY)
    for row in rows:
        tables[row["table_name"]] = {
            "row_estimate": row["row_estimate"],
            "size_bytes": row["size_bytes"],
            "indexes": [],
            "columns": [],
        }

    # 2. Index definitions
    idx_rows = await conn.fetch(_ALL_INDEX_QUERY)
    for row in idx_rows:
        tname = row["table_name"]
        if tname not in tables:
            continue
        tables[tname]["indexes"].append(
            {
                "name": row["index_name"],
                "columns": list(row["columns"] or []),
                "include_columns": list(row["include_columns"] or []),
                "is_unique": row["is_unique"],
                "is_partial": row["predicate"] is not None,
                "index_type": row["index_type"],
            }
        )

    # 3. Column statistics
    col_rows = await conn.fetch(_ALL_COLUMN_STATS_QUERY)
    for row in col_rows:
        tname = row["table_name"]
        if tname not in tables:
            continue
        tables[tname]["columns"].append(
            {
                "name": row["column_name"],
                "type": row["column_type"],
                "null_fraction": row["null_fraction"],
                "distinct_count": row["distinct_count"],
            }
        )

    return tables


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def format_stats_comments(stats: dict[str, dict]) -> str:
    """Serialize stats as ``-- pgreviewer:stats`` SQL comments.

    One line per table, each containing a JSON object with the table name
    as the key.  This is grep-friendly and diff-friendly.
    """
    lines: list[str] = []
    lines.append("-- pgreviewer:meta schema dump with statistics")
    for table_name in sorted(stats):
        payload = json.dumps(
            {table_name: stats[table_name]},
            separators=(",", ":"),
        )
        lines.append(f"-- pgreviewer:stats {payload}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def dump_schema(
    database_url: str, output: Path, *, no_stats: bool = False
) -> None:
    """Run pg_dump and (optionally) collect stats, then write to *output*.

    Parameters
    ----------
    database_url:
        PostgreSQL connection URI.
    output:
        Destination file path.
    no_stats:
        When ``True``, skip stats collection and write DDL only.
    """
    ddl = run_pg_dump(database_url)

    stats_section = ""
    if not no_stats:
        import asyncpg

        conn = await asyncpg.connect(database_url)
        try:
            stats = await collect_all_stats(conn)
            stats_section = "\n" + format_stats_comments(stats)
        finally:
            await conn.close()

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(ddl + stats_section)
