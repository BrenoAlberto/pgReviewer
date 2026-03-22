"""Detect CREATE INDEX statements that duplicate an existing schema index.

A redundant index is one whose leading columns are already a prefix of
an existing index on the same table — the new index adds no query
acceleration that the existing one doesn't already provide, but incurs
extra write overhead and storage cost on every INSERT/UPDATE/DELETE.

This detector is **schema-aware only**: without a base schema there is
no way to know which indexes already exist, so it silently skips when
``schema.tables`` is empty.

Examples of redundancy:
- Schema has ``(user_id, created_at)``, PR creates ``(user_id)`` — the
  existing index already covers single-column lookups on ``user_id``.
- Schema has ``(email)`` unique, PR creates ``(email)`` non-unique.

Severity is always WARNING — redundant indexes are harmful but rarely
an emergency.
"""

from __future__ import annotations

import re

from pgreviewer.analysis.migration_detectors import BaseMigrationDetector
from pgreviewer.core.models import Issue, ParsedMigration, SchemaInfo, Severity

_CREATE_INDEX_RE = re.compile(
    r"\bCREATE\s+(?P<unique>UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?"
    r"(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>[^\s(]+)\s+ON\s+"
    r"(?:ONLY\s+)?(?P<table>[^\s(]+)\s*\((?P<columns>[^)]+)\)",
    re.IGNORECASE,
)


def _normalize(identifier: str) -> str:
    """Strip schema prefix, quotes, and lower-case."""
    return identifier.rsplit(".", 1)[-1].strip().strip('"').lower()


def _parse_columns(cols_str: str) -> list[str]:
    return [_normalize(c.split()[0]) for c in cols_str.split(",") if c.strip()]


class RedundantIndexDetector(BaseMigrationDetector):
    @property
    def name(self) -> str:
        return "redundant_index"

    def detect(self, migration: ParsedMigration, schema: SchemaInfo) -> list[Issue]:
        # Schema-aware only — skip silently in degraded-static mode
        if not schema.tables:
            return []

        issues: list[Issue] = []

        for stmt in migration.statements:
            if stmt.statement_type != "CREATE INDEX":
                continue

            m = _CREATE_INDEX_RE.search(stmt.raw_sql)
            if not m:
                continue

            table = _normalize(m.group("table"))
            new_cols = _parse_columns(m.group("columns"))
            new_name = _normalize(m.group("name"))

            if not new_cols or table not in schema.tables:
                continue

            covering = self._find_covering_index(table, new_cols, new_name, schema)
            if covering is None:
                continue

            issues.append(
                Issue(
                    severity=Severity.WARNING,
                    detector_name=self.name,
                    description=(
                        f"Index '{new_name}' on '{table}{new_cols}' is redundant: "
                        f"existing index '{covering}' already covers these columns "
                        "as a leading prefix. The new index adds write overhead "
                        "without query benefit."
                    ),
                    affected_table=table,
                    affected_columns=new_cols,
                    suggested_action=(
                        f"Drop the new index '{new_name}' and rely on '{covering}' "
                        "instead. If the new index is intended for a specific query "
                        "pattern, add a COMMENT to explain why it is not redundant."
                    ),
                    fix_type="replace",
                    context={"line_number": stmt.line_number},
                )
            )

        return issues

    def _find_covering_index(
        self,
        table: str,
        new_cols: list[str],
        new_name: str,
        schema: SchemaInfo,
    ) -> str | None:
        """Return the name of an existing index that makes *new_cols* redundant."""
        for idx in schema.tables[table].indexes:
            if _normalize(idx.name) == new_name:
                # Same name — allow re-creation (e.g., idempotent migration)
                continue
            if not idx.columns:
                continue
            existing_cols = [_normalize(c) for c in idx.columns]
            # new_cols is redundant if it is a prefix of existing_cols
            n = len(new_cols)
            if n <= len(existing_cols) and existing_cols[:n] == new_cols:
                return idx.name
        return None
