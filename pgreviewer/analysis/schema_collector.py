"""Collects schema metadata (table stats, indexes, column statistics) from PostgreSQL.

Sources:
    - ``pg_class`` for row estimates and table sizes
    - ``pg_indexes`` for index definitions
    - ``pg_stats`` for column statistics
"""

from __future__ import annotations

from fnmatch import fnmatch
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg

from pgreviewer.core.models import ColumnInfo, IndexInfo, SchemaInfo, TableInfo

_TABLE_STATS_QUERY = """
SELECT
    c.relname                       AS table_name,
    c.reltuples::bigint             AS row_estimate,
    pg_relation_size(c.oid)         AS size_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = ANY($1::text[])
  AND n.nspname = 'public'
  AND c.relkind = 'r';
"""

_INDEX_QUERY = """
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
CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS x(attnum, ordinality)
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = x.attnum
WHERE t.relname = ANY($1::text[])
  AND n.nspname = 'public'
  AND x.attnum > 0
GROUP BY
    ix.relname, t.relname, i.indisunique, i.indpred,
    i.indrelid, i.indnkeyatts, am.amname;
"""

_COLUMN_STATS_QUERY = """
SELECT
    s.tablename                     AS table_name,
    s.attname                       AS column_name,
    format_type(a.atttypid, a.atttypmod) AS column_type,
    s.null_frac                     AS null_fraction,
    s.n_distinct                    AS distinct_count
FROM pg_stats s
JOIN pg_class c ON c.relname = s.tablename
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = s.schemaname
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = s.attname
WHERE s.tablename = ANY($1::text[])
  AND s.schemaname = 'public';
"""

# Module-level cache keyed on frozenset of table names.
_cache: dict[frozenset[str], SchemaInfo] = {}


def clear_cache() -> None:
    """Reset the module-level schema cache."""
    _cache.clear()


async def collect_schema(
    tables: list[str],
    conn: asyncpg.Connection,
    ignored_table_patterns: list[str] | None = None,
) -> SchemaInfo:
    """Collect schema metadata for *tables* from the connected PostgreSQL instance.

    Results are cached by the set of requested table names so that multiple
    calls within a single ``pgr check`` run do not issue redundant queries.

    Parameters
    ----------
    tables:
        Table names (``public`` schema) to collect stats for.
    conn:
        An open :class:`asyncpg.Connection`.

    Returns
    -------
    SchemaInfo
        Populated with :class:`TableInfo` entries for every requested table
        that exists in the database.
    """
    active_tables = tables
    if ignored_table_patterns:
        active_tables = [
            table
            for table in tables
            if not any(
                fnmatch(table.lower(), pattern.lower())
                for pattern in ignored_table_patterns
            )
        ]

    cache_key = frozenset(active_tables)
    if cache_key in _cache:
        return _cache[cache_key]

    table_map: dict[str, TableInfo] = {}

    # 1. Table-level stats (row estimates, sizes) --------------------------
    rows = await conn.fetch(_TABLE_STATS_QUERY, active_tables)
    for row in rows:
        table_map[row["table_name"]] = TableInfo(
            row_estimate=row["row_estimate"],
            size_bytes=row["size_bytes"],
        )

    # Ensure entries exist for tables that were requested but had no stats.
    for t in active_tables:
        table_map.setdefault(t, TableInfo())

    # 2. Index definitions -------------------------------------------------
    idx_rows = await conn.fetch(_INDEX_QUERY, active_tables)
    for row in idx_rows:
        tname = row["table_name"]
        table_map[tname].indexes.append(
            IndexInfo(
                name=row["index_name"],
                columns=list(row["columns"] or []),
                include_columns=list(row["include_columns"] or []),
                is_unique=row["is_unique"],
                is_partial=row["predicate"] is not None,
                index_type=row["index_type"],
            )
        )

    # 3. Column statistics -------------------------------------------------
    col_rows = await conn.fetch(_COLUMN_STATS_QUERY, active_tables)
    for row in col_rows:
        tname = row["table_name"]
        table_map[tname].columns.append(
            ColumnInfo(
                name=row["column_name"],
                type=row["column_type"],
                null_fraction=row["null_fraction"],
                distinct_count=row["distinct_count"],
            )
        )

    schema = SchemaInfo(tables=table_map)
    _cache[cache_key] = schema
    return schema
