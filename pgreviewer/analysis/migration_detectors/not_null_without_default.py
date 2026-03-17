import re

from pgreviewer.analysis.migration_detectors import BaseMigrationDetector
from pgreviewer.config import settings
from pgreviewer.core.models import Issue, ParsedMigration, SchemaInfo, Severity

# ADD COLUMN ... NOT NULL (without DEFAULT)
# We capture the entire ADD COLUMN action to check for both NOT NULL and DEFAULT.
_ADD_COLUMN_RE = re.compile(
    r"""
    \bADD\s+COLUMN\b
    (?:\s+IF\s+NOT\s+EXISTS)?
    \s+(?P<column>(?:"[^"]+"|[^\s]+))
    \s+(?P<action>.*?)
    (?=
        \s*,
        |\s*ALTER\s+COLUMN\b
        |\s*DROP\s+COLUMN\b
        |\s*ADD\s+CONSTRAINT\b
        |\s*;
        |$
    )
    """,
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)

# ALTER COLUMN ... SET NOT NULL
_SET_NOT_NULL_RE = re.compile(
    r"\bALTER\s+COLUMN\s+(?P<column>(?:\"[^\"]+\"|[^\s]+))\s+SET\s+NOT\s+NULL\b",
    re.IGNORECASE | re.VERBOSE,
)


def _normalize_identifier(identifier: str) -> str:
    return identifier.strip().strip('"')


class NotNullWithoutDefaultDetector(BaseMigrationDetector):
    @property
    def name(self) -> str:
        return "add_not_null_without_default"

    def detect(self, migration: ParsedMigration, schema: SchemaInfo) -> list[Issue]:
        issues: list[Issue] = []
        for statement in migration.statements:
            if statement.statement_type != "ALTER TABLE":
                continue

            raw_sql = statement.raw_sql

            # 1. Check ADD COLUMN ... NOT NULL
            for match in _ADD_COLUMN_RE.finditer(raw_sql):
                action = match.group("action").upper()
                if "NOT NULL" in action and "DEFAULT" not in action:
                    issues.append(
                        self._create_issue(
                            statement,
                            migration,
                            match.group("column"),
                            schema,
                            is_add_column=True,
                        )
                    )

            # 2. Check ALTER COLUMN ... SET NOT NULL
            for match in _SET_NOT_NULL_RE.finditer(raw_sql):
                column_raw = match.group("column")
                issues.append(
                    self._create_issue(
                        statement,
                        migration,
                        column_raw,
                        schema,
                        is_add_column=False,
                    )
                )

        return issues

    def _create_issue(
        self,
        statement,
        migration: ParsedMigration,
        column_raw: str,
        schema: SchemaInfo,
        is_add_column: bool,
    ) -> Issue:
        table = statement.table
        column = _normalize_identifier(column_raw)
        row_estimate = 0
        if table and table in schema.tables:
            row_estimate = schema.tables[table].row_estimate

        severity = Severity.WARNING
        if row_estimate > settings.SEQ_SCAN_ROW_THRESHOLD:
            severity = Severity.CRITICAL

        if is_add_column:
            reason = f"ADD COLUMN '{column}' NOT NULL without DEFAULT"
            approach = "Add nullable, backfill, then add NOT NULL constraint"
            suggested = (
                "Adding a NOT NULL column without a default value requires a "
                "full table scan to validate existing rows (which are all NULL). "
                "Two-phase approach: 1) Add the column as nullable. 2) Backfill the "
                "column in batches. 3) Add the NOT NULL constraint."
            )
        else:
            reason = f"ALTER COLUMN '{column}' SET NOT NULL"
            approach = "Use NOT VALID check constraint then validate"
            suggested = (
                "Changing a column to NOT NULL requires a full table scan to validate "
                "that no NULLs exist. "
                "Alternative: Use a CHECK constraint with (column IS NOT NULL) "
                "NOT VALID, then VALIDATE CONSTRAINT separately to avoid taking "
                "a long-held Access Exclusive lock."
            )

        return Issue(
            severity=severity,
            detector_name=self.name,
            description=(
                f"{reason} detected on table '{table}' in {migration.source_file}. "
                f"Estimated rows: {row_estimate}. Suggested approach: {approach}."
            ),
            affected_table=table,
            affected_columns=[column],
            suggested_action=suggested,
            context={
                "line_number": statement.line_number,
                "row_estimate": row_estimate,
            },
        )
