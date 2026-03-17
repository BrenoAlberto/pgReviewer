import re
from pathlib import Path

from pgreviewer.analysis.migration_detectors import BaseMigrationDetector
from pgreviewer.config import settings
from pgreviewer.core.models import Issue, ParsedMigration, SchemaInfo, Severity

# CREATE [UNIQUE] INDEX [CONCURRENTLY] name ON table (...)
_CREATE_INDEX_RE = re.compile(
    r"\bCREATE\s+(?P<unique>UNIQUE\s+)?INDEX\s+(?P<concurrently>CONCURRENTLY\s+)?(?P<name>[^\s(]+)\s+ON\s+(?P<table>[^\s(]+)",
    re.IGNORECASE | re.VERBOSE,
)


def _normalize_identifier(identifier: str) -> str:
    return identifier.strip().strip('"')


class IndexNotConcurrentlyDetector(BaseMigrationDetector):
    @property
    def name(self) -> str:
        return "create_index_not_concurrently"

    def detect(self, migration: ParsedMigration, schema: SchemaInfo) -> list[Issue]:
        issues: list[Issue] = []

        is_transactional = self._check_if_transactional(migration)

        for statement in migration.statements:
            if statement.statement_type != "CREATE INDEX":
                continue

            match = _CREATE_INDEX_RE.search(statement.raw_sql)
            if not match:
                continue

            has_concurrently = bool(match.group("concurrently"))
            table_raw = match.group("table")
            table = _normalize_identifier(table_raw)

            row_estimate = 0
            if table and table in schema.tables:
                row_estimate = schema.tables[table].row_estimate

            if not has_concurrently:
                severity = Severity.WARNING
                if row_estimate > settings.CONCURRENT_INDEX_THRESHOLD:
                    severity = Severity.CRITICAL

                # Construct replacement SQL
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
                            "block. Ensure the migration is configured to run without "
                            "a transaction (e.g., autocommit or Alembic's "
                            "with_operations_in_transaction=False)."
                        ),
                        context={"line_number": statement.line_number},
                    )
                )

        return issues

    def _check_if_transactional(self, migration: ParsedMigration) -> bool:
        """Heuristic to check if the migration file is transactional."""
        try:
            if not migration.source_file:
                return False

            p = Path(migration.source_file)
            if not p.exists():
                return False

            content = p.read_text().upper()
            # Simple check for BEGIN/COMMIT
            if "BEGIN" in content or "START TRANSACTION" in content:
                return True

            # Alembic files are almost always transactional by default
            if migration.source_file.endswith(".py") and "ALEMBIC" in content:
                return True

        except Exception:
            pass
        return False
