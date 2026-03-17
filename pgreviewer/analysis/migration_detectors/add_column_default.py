import re

from pgreviewer.analysis.migration_detectors import BaseMigrationDetector
from pgreviewer.config import settings
from pgreviewer.core.models import Issue, ParsedMigration, SchemaInfo, Severity

_ADD_COLUMN_DEFAULT_RE = re.compile(
    r"""
    \bADD\s+COLUMN\b
    (?:\s+IF\s+NOT\s+EXISTS)?
    \s+(?P<column>(?:"[^"]+"|[^\s]+))
    .*?
    \bDEFAULT\b
    \s+(?P<default>.+?)
    (?=
        \s+NOT\s+NULL
        |\s+NULL
        |\s+CONSTRAINT\b
        |\s+CHECK\b
        |\s+PRIMARY\b
        |\s+REFERENCES\b
        |\s*;
        |$
    )
    """,
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)
_LITERAL_DEFAULT_RE = re.compile(
    r"""
    ^
    (?:
        NULL
        |TRUE
        |FALSE
        |[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?
        |'(?:''|[^'])*'
    )
    (?:\s*::\s*[\w\.\[\]"]+)?
    $
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _normalize_identifier(identifier: str) -> str:
    return identifier.strip().strip('"')


def _is_immutable_literal(default_expr: str) -> bool:
    normalized = default_expr.strip()
    return bool(_LITERAL_DEFAULT_RE.fullmatch(normalized))


class AddColumnDefaultDetector(BaseMigrationDetector):
    @property
    def name(self) -> str:
        return "add_column_with_default"

    def detect(self, migration: ParsedMigration, schema: SchemaInfo) -> list[Issue]:
        del schema

        issues: list[Issue] = []
        for statement in migration.statements:
            if statement.statement_type != "ALTER TABLE":
                continue

            match = _ADD_COLUMN_DEFAULT_RE.search(statement.raw_sql)
            if not match:
                continue

            default_expr = match.group("default").strip()
            column = _normalize_identifier(match.group("column"))

            if settings.POSTGRES_VERSION < 11:
                severity = Severity.CRITICAL
                suggested_action = (
                    "On PostgreSQL < 11, ADD COLUMN with DEFAULT rewrites the whole "
                    "table and takes an ACCESS EXCLUSIVE lock. Split into add-nullable "
                    "column, backfill in batches, then set DEFAULT/NOT NULL."
                )
            elif _is_immutable_literal(default_expr):
                severity = Severity.INFO
                suggested_action = (
                    "On PostgreSQL 11+, immutable literal defaults are metadata-only "
                    "and avoid a full table rewrite."
                )
            else:
                severity = Severity.WARNING
                suggested_action = (
                    "On PostgreSQL 11+, volatile or non-literal defaults can still "
                    "force a table rewrite. Consider splitting the migration and "
                    "backfilling in batches."
                )

            issues.append(
                Issue(
                    severity=severity,
                    detector_name=self.name,
                    description=(
                        "ALTER TABLE ... ADD COLUMN ... DEFAULT detected in "
                        f"{migration.source_file} (default: {default_expr})."
                    ),
                    affected_table=statement.table,
                    affected_columns=[column],
                    suggested_action=suggested_action,
                    context={"line_number": statement.line_number},
                )
            )
        return issues
