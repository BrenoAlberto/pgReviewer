from __future__ import annotations

import re
from pathlib import Path

from pgreviewer.analysis.migration_detectors import BaseMigrationDetector
from pgreviewer.config import settings
from pgreviewer.core.models import Issue, ParsedMigration, SchemaInfo, Severity

# CREATE [UNIQUE] INDEX [CONCURRENTLY] name ON table (...)
_CREATE_INDEX_RE = re.compile(
    r"\bCREATE\s+(?P<unique>UNIQUE\s+)?INDEX\s+(?P<concurrently>CONCURRENTLY\s+)?"
    r"(?P<name>[^\s(]+)\s+ON\s+(?P<table>[^\s(]+)",
    re.IGNORECASE,
)

# DROP INDEX [CONCURRENTLY] [IF EXISTS] name
_DROP_INDEX_RE = re.compile(
    r"\bDROP\s+INDEX\s+(?P<concurrently>CONCURRENTLY\s+)?"
    r"(?:IF\s+EXISTS\s+)?(?P<name>[^\s(;,]+)",
    re.IGNORECASE,
)


def _normalize_identifier(identifier: str) -> str:
    return identifier.strip().strip('"')


def _check_if_transactional(migration: ParsedMigration) -> bool:
    """Heuristic: returns True if the migration file runs inside a transaction."""
    try:
        if not migration.source_file:
            return False
        p = Path(migration.source_file)
        if not p.exists():
            return False
        content = p.read_text().upper()
        if "BEGIN" in content or "START TRANSACTION" in content:
            return True
        if migration.source_file.endswith(".py") and "ALEMBIC" in content:
            return True
    except Exception:
        pass
    return False


def _build_drop_index_replacement(raw_sql: str) -> str:
    """Return DROP INDEX SQL with CONCURRENTLY and IF EXISTS added."""
    s = re.sub(r"\bCONCURRENTLY\b", "", raw_sql, flags=re.IGNORECASE)
    s = re.sub(r"\bIF\s+EXISTS\b", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return re.sub(
        r"\bDROP\s+INDEX\b",
        "DROP INDEX CONCURRENTLY IF EXISTS",
        s,
        count=1,
        flags=re.IGNORECASE,
    )


class IndexNotConcurrentlyDetector(BaseMigrationDetector):
    @property
    def name(self) -> str:
        return "create_index_not_concurrently"

    def detect(self, migration: ParsedMigration, schema: SchemaInfo) -> list[Issue]:
        issues: list[Issue] = []
        is_transactional = _check_if_transactional(migration)

        for statement in migration.statements:
            if statement.statement_type != "CREATE INDEX":
                continue

            match = _CREATE_INDEX_RE.search(statement.raw_sql)
            if not match:
                continue

            has_concurrently = bool(match.group("concurrently"))
            table = _normalize_identifier(match.group("table"))

            row_estimate = 0
            if table and table in schema.tables:
                row_estimate = schema.tables[table].row_estimate

            if not has_concurrently:
                severity = Severity.WARNING
                if row_estimate > settings.CONCURRENT_INDEX_THRESHOLD:
                    severity = Severity.CRITICAL

                replacement_sql = statement.raw_sql.replace(
                    "INDEX", "INDEX CONCURRENTLY", 1
                )
                issues.append(
                    Issue(
                        severity=severity,
                        detector_name=self.name,
                        description=(
                            f"CREATE INDEX on '{table}' without CONCURRENTLY "
                            f"detected in {migration.source_file}. "
                            f"Estimated rows: {row_estimate}."
                        ),
                        affected_table=table,
                        affected_columns=[],
                        suggested_action=(
                            "Use CONCURRENTLY to avoid blocking writers: "
                            f"{replacement_sql}. Note: CONCURRENTLY cannot "
                            "run inside a transaction block."
                        ),
                        context={"line_number": statement.line_number},
                    )
                )
            elif is_transactional:
                issues.append(
                    Issue(
                        severity=Severity.WARNING,
                        detector_name=self.name,
                        description=(
                            f"CREATE INDEX CONCURRENTLY detected inside a "
                            f"transactional migration ({migration.source_file})."
                        ),
                        affected_table=table,
                        affected_columns=[],
                        suggested_action=(
                            "CREATE INDEX CONCURRENTLY cannot run inside a transaction "
                            "block. Wrap in op.get_context().autocommit_block() or "
                            "split into a separate non-transactional migration."
                        ),
                        context={"line_number": statement.line_number},
                    )
                )

        return issues


class DropIndexNotConcurrentlyDetector(BaseMigrationDetector):
    @property
    def name(self) -> str:
        return "drop_index_not_concurrently"

    def detect(self, migration: ParsedMigration, schema: SchemaInfo) -> list[Issue]:
        issues: list[Issue] = []
        is_transactional = _check_if_transactional(migration)

        for statement in migration.statements:
            if statement.statement_type != "DROP INDEX":
                continue

            match = _DROP_INDEX_RE.search(statement.raw_sql)
            if not match:
                continue

            has_concurrently = bool(match.group("concurrently"))
            index_name = _normalize_identifier(match.group("name").rstrip(";"))

            if not has_concurrently:
                replacement_sql = _build_drop_index_replacement(statement.raw_sql)
                issues.append(
                    Issue(
                        severity=Severity.WARNING,
                        detector_name=self.name,
                        description=(
                            f"DROP INDEX on '{index_name}' without CONCURRENTLY "
                            f"detected in {migration.source_file}."
                        ),
                        affected_table=statement.table,
                        affected_columns=[],
                        suggested_action=(
                            "Use CONCURRENTLY to avoid blocking writers: "
                            f"{replacement_sql}. Note: CONCURRENTLY cannot "
                            "run inside a transaction block."
                        ),
                        context={
                            "line_number": statement.line_number,
                            "index_name": index_name,
                        },
                    )
                )
            elif is_transactional:
                issues.append(
                    Issue(
                        severity=Severity.WARNING,
                        detector_name=self.name,
                        description=(
                            f"DROP INDEX CONCURRENTLY detected inside a "
                            f"transactional migration ({migration.source_file})."
                        ),
                        affected_table=statement.table,
                        affected_columns=[],
                        suggested_action=(
                            "DROP INDEX CONCURRENTLY cannot run inside a transaction "
                            "block. Wrap in op.get_context().autocommit_block() or "
                            "split into a separate non-transactional migration."
                        ),
                        context={
                            "line_number": statement.line_number,
                            "index_name": index_name,
                        },
                    )
                )

        return issues
