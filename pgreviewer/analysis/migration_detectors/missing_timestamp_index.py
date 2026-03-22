"""Detect timestamp/timestamptz columns added without a corresponding index.

Timestamp columns are frequently used in range queries (``WHERE created_at > X``,
``ORDER BY updated_at DESC``).  Adding one without an index is a common oversight
that surfaces as slow queries in production.

Severity:
- **WARNING** (degraded-static): no schema loaded — cannot check existing indexes
  or table size; emits with a note that schema data would improve precision.
- **WARNING** (schema loaded, small table): table is below the concurrency index
  threshold; useful to flag but not yet urgent.
- **CRITICAL** (schema loaded, large table): table exceeds
  ``CONCURRENT_INDEX_THRESHOLD`` rows; missing index on a large table is likely
  causing real performance impact.

The detector suppresses findings when:
- An index already exists on the timestamp column in the base schema.
- A ``CREATE INDEX`` statement in the same migration covers the column.
"""

from __future__ import annotations

import re

from pgreviewer.analysis.migration_detectors import BaseMigrationDetector
from pgreviewer.config import settings
from pgreviewer.core.models import Issue, ParsedMigration, SchemaInfo, Severity

# Timestamp-like type names (covers bare type and schema-qualified variants)
_TIMESTAMP_TYPES = re.compile(
    r"\b(timestamp(?:tz)?(?:\s+with(?:out)?\s+time\s+zone)?)\b",
    re.IGNORECASE,
)

# ADD COLUMN name type ... — captures name and full type fragment
_ADD_COLUMN_RE = re.compile(
    r"\bADD\s+COLUMN\b(?:\s+IF\s+NOT\s+EXISTS)?\s+"
    r"(?P<name>(?:\"[^\"]+\"|[^\s]+))\s+"
    r"(?P<type_frag>\S+(?:\s+\S+){0,5})",
    re.IGNORECASE,
)

# CREATE INDEX ... ON table (col, ...)
_CREATE_INDEX_RE = re.compile(
    r"\bCREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?"
    r"(?P<name>[^\s(]+)\s+ON\s+(?P<table>[^\s(]+)\s*\((?P<columns>[^)]+)\)",
    re.IGNORECASE,
)


def _normalize(identifier: str) -> str:
    return identifier.strip().strip('"').lower()


def _parse_columns(cols_str: str) -> list[str]:
    return [_normalize(c) for c in cols_str.split(",") if c.strip()]


class MissingTimestampIndexDetector(BaseMigrationDetector):
    @property
    def name(self) -> str:
        return "missing_timestamp_index"

    def detect(self, migration: ParsedMigration, schema: SchemaInfo) -> list[Issue]:
        issues: list[Issue] = []

        # 1. Collect indexes created in this migration (table → set of column names)
        migration_indexes: dict[str, set[str]] = {}
        for stmt in migration.statements:
            if stmt.statement_type != "CREATE INDEX":
                continue
            m = _CREATE_INDEX_RE.search(stmt.raw_sql)
            if m:
                table = _normalize(m.group("table"))
                cols = _parse_columns(m.group("columns"))
                migration_indexes.setdefault(table, set()).update(cols)

        # 2. Scan ALTER TABLE ADD COLUMN for timestamp columns
        for stmt in migration.statements:
            if stmt.statement_type != "ALTER TABLE" or not stmt.table:
                continue
            table = _normalize(stmt.table)

            for m in _ADD_COLUMN_RE.finditer(stmt.raw_sql):
                col_name = _normalize(m.group("name"))
                type_frag = m.group("type_frag")

                if not _TIMESTAMP_TYPES.search(type_frag):
                    continue

                # Skip if this migration already indexes the column
                if col_name in migration_indexes.get(table, set()):
                    continue

                # Skip if schema already has an index covering this column
                if self._is_indexed_in_schema(table, col_name, schema):
                    continue

                row_estimate, table_in_schema = self._row_estimate(table, schema)
                severity = self._severity(schema, table_in_schema, row_estimate)
                description = self._description(
                    col_name,
                    table,
                    row_estimate,
                    schema,
                    stmt.source_file
                    if hasattr(stmt, "source_file")
                    else migration.source_file,
                )

                issues.append(
                    Issue(
                        severity=severity,
                        detector_name=self.name,
                        description=description,
                        affected_table=table,
                        affected_columns=[col_name],
                        suggested_action=(
                            f"Add an index on '{col_name}' to support range "
                            "and ORDER BY queries: "
                            f"CREATE INDEX CONCURRENTLY idx_{table}_{col_name} "
                            f"ON {table} ({col_name});"
                        ),
                        fix_type="additive",
                        context={
                            "line_number": stmt.line_number,
                            "row_estimate": row_estimate,
                        },
                    )
                )

        return issues

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_indexed_in_schema(
        self, table: str, col_name: str, schema: SchemaInfo
    ) -> bool:
        if table not in schema.tables:
            return False
        for idx in schema.tables[table].indexes:
            if idx.columns and _normalize(idx.columns[0]) == col_name:
                return True
        return False

    def _row_estimate(self, table: str, schema: SchemaInfo) -> tuple[int, bool]:
        if table in schema.tables:
            return schema.tables[table].row_estimate, True
        return 0, False

    def _severity(
        self, schema: SchemaInfo, table_in_schema: bool, row_estimate: int
    ) -> Severity:
        if not schema.tables:
            return Severity.WARNING
        if not table_in_schema:
            # New table created in this PR — can't gauge size
            return Severity.WARNING
        if row_estimate > settings.CONCURRENT_INDEX_THRESHOLD:
            return Severity.CRITICAL
        return Severity.WARNING

    def _description(
        self,
        col_name: str,
        table: str,
        row_estimate: int,
        schema: SchemaInfo,
        source_file: str,
    ) -> str:
        base = (
            f"Timestamp column '{col_name}' added to table '{table}' "
            f"in {source_file} without a corresponding index. "
            f"Estimated rows: {row_estimate}. "
            "Timestamp columns are commonly range-queried and should be indexed."
        )
        if not schema.tables:
            base += (
                " (severity may be higher with schema data — "
                "commit .pgreviewer/schema.sql for full analysis)"
            )
        return base
