import re

from pgreviewer.analysis.migration_detectors import BaseMigrationDetector
from pgreviewer.config import settings
from pgreviewer.core.models import Issue, ParsedMigration, SchemaInfo, Severity

# ALTER [COLUMN] col TYPE new_type [USING expression]
_ALTER_COLUMN_TYPE_RE = re.compile(
    r"ALTER\s+(?:COLUMN\s+)?(?P<column>(?:\"[^\"]+\"|[^\s]+))\s+TYPE\s+(?P<type>[^;,\s]+(?:\([\d\s,]+\))?)(?:\s+USING\s+(?P<using>[^;,]+))?",
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)


def _normalize_identifier(identifier: str) -> str:
    return identifier.strip().strip('"')


class AlterColumnTypeDetector(BaseMigrationDetector):
    @property
    def name(self) -> str:
        return "alter_column_type"

    def detect(self, migration: ParsedMigration, schema: SchemaInfo) -> list[Issue]:
        issues: list[Issue] = []

        for statement in migration.statements:
            if statement.statement_type != "ALTER TABLE":
                continue

            table = statement.table
            if not table:
                continue
            table = _normalize_identifier(table)

            row_estimate = 0
            if table in schema.tables:
                row_estimate = schema.tables[table].row_estimate

            for match in _ALTER_COLUMN_TYPE_RE.finditer(statement.raw_sql):
                column = _normalize_identifier(match.group("column"))
                new_type = match.group("type").strip().upper()
                has_using = bool(match.group("using"))

                is_safe = self._is_type_change_safe(
                    table, column, new_type, has_using, schema
                )

                if not is_safe:
                    severity = Severity.WARNING
                    if row_estimate > settings.TABLE_REWRITE_THRESHOLD:
                        severity = Severity.CRITICAL

                    issues.append(
                        Issue(
                            severity=severity,
                            detector_name=self.name,
                            description=(
                                f"Altering column '{column}' type to '{new_type}' "
                                f"on table '{table}' requires a full table rewrite "
                                "and an AccessExclusiveLock."
                            ),
                            affected_table=table,
                            affected_columns=[column],
                            suggested_action=(
                                "Consider a multi-step approach: 1. Add new column "
                                "with desired type, 2. Backfill data in batches, "
                                "3. Atomically rename columns and drop old one."
                            ),
                            context={
                                "line_number": statement.line_number,
                                "row_estimate": row_estimate,
                            },
                        )
                    )

        return issues

    def _is_type_change_safe(
        self,
        table: str,
        column: str,
        new_type: str,
        has_using: bool,
        schema: SchemaInfo,
    ) -> bool:
        """Determines if a type change is safe (no table rewrite)."""
        # 1. Any change with USING clause is considered unsafe by default here
        if has_using:
            return False

        # 2. Get old type from schema
        old_type = None
        if table in schema.tables:
            for col_info in schema.tables[table].columns:
                if col_info.name == column:
                    old_type = col_info.type.upper()
                    break

        if not old_type:
            # If we don't know the old type, we act conservatively and flag it
            return False

        # 3. Handle VARCHAR(N) -> VARCHAR(M) where M > N
        varchar_re = re.compile(r"VARCHAR\s*\(\s*(\d+)\s*\)", re.IGNORECASE)
        old_varchar_match = varchar_re.match(old_type)
        new_varchar_match = varchar_re.match(new_type)

        if old_varchar_match and new_varchar_match:
            old_size = int(old_varchar_match.group(1))
            new_size = int(new_varchar_match.group(1))
            if new_size >= old_size:
                return True

        # 4. Same for VARBIT(N) -> VARBIT(M) (less common but same logic)
        varbit_re = re.compile(r"VARBIT\s*\(\s*(\d+)\s*\)", re.IGNORECASE)
        old_varbit_match = varbit_re.match(old_type)
        new_varbit_match = varbit_re.match(new_type)
        if old_varbit_match and new_varbit_match:
            old_size = int(old_varbit_match.group(1))
            new_size = int(new_varbit_match.group(1))
            if new_size >= old_size:
                return True

        # 5. VARCHAR -> TEXT is often safe but user requested it to be unsafe
        if old_type.startswith("VARCHAR") and new_type == "TEXT":
            return False

        # 6. Default: unsafe
        return False
