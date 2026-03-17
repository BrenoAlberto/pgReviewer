import re

from pgreviewer.analysis.migration_detectors import BaseMigrationDetector
from pgreviewer.core.models import (
    ExtractedQuery,
    Issue,
    ParsedMigration,
    SchemaInfo,
    Severity,
)

_DROP_COLUMN_RE = re.compile(
    r"""
    ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?P<table>[^\s(]+)\s+
    DROP\s+COLUMN\s+(?:IF\s+EXISTS\s+)?(?P<column>"[^"]+"|[a-zA-Z_][\w$]*)
    """,
    re.IGNORECASE | re.VERBOSE,
)
_REFERENCES_RE_TEMPLATE = r"\bREFERENCES\s+{table}\s*\((?P<columns>[^)]+)\)"


def _normalize_identifier(identifier: str) -> str:
    return identifier.strip().strip('"').lower()


def _contains_identifier(sql: str, identifier: str) -> bool:
    escaped_identifier = re.escape(identifier)
    pattern = re.compile(
        rf'(?<![\w$])(?:"{escaped_identifier}"|{escaped_identifier})(?![\w$])',
        re.IGNORECASE,
    )
    return pattern.search(sql) is not None


def _references_table_column(
    sql: str, table_identifiers: set[str], column: str
) -> bool:
    return any(
        _contains_identifier(sql, table_identifier)
        for table_identifier in table_identifiers
    ) and _contains_identifier(sql, column)


class DropColumnStillReferencedDetector(BaseMigrationDetector):
    @property
    def name(self) -> str:
        return "drop_column_still_referenced"

    def detect(self, migration: ParsedMigration, schema: SchemaInfo) -> list[Issue]:
        del schema

        issues: list[Issue] = []
        for statement in migration.statements:
            match = _DROP_COLUMN_RE.search(statement.raw_sql)
            if not match:
                continue

            raw_table = _normalize_identifier(match.group("table"))
            table = raw_table.split(".")[-1]
            table_identifiers = {raw_table, table}
            column = _normalize_identifier(match.group("column"))
            reference = f"{table}.{column}"
            query_references = self._find_query_references(
                migration.extracted_queries,
                migration.source_file,
                statement.line_number,
                statement.raw_sql,
                table_identifiers,
                column,
            )

            if query_references:
                referenced_files = ", ".join(
                    f"{q.source_file}:{q.line_number}" for q in query_references
                )
                migration_file = migration.source_file
                issues.append(
                    Issue(
                        severity=Severity.CRITICAL,
                        detector_name=self.name,
                        description=(
                            f"DROP COLUMN {reference} in {migration_file} is still "
                            f"referenced by extracted queries: {referenced_files}"
                        ),
                        affected_table=table,
                        affected_columns=[column],
                        suggested_action=(
                            f"Search the codebase for references to `{reference}` "
                            "before dropping"
                        ),
                        context={
                            "line_number": statement.line_number,
                            "query_references": [
                                {
                                    "source_file": q.source_file,
                                    "line_number": q.line_number,
                                }
                                for q in query_references
                            ],
                        },
                    )
                )
                continue

            if self._is_fk_target(
                migration.extracted_queries, table_identifiers, column
            ):
                migration_file = migration.source_file
                issues.append(
                    Issue(
                        severity=Severity.WARNING,
                        detector_name=self.name,
                        description=(
                            f"DROP COLUMN {reference} in {migration_file} may be a "
                            "foreign key target referenced by other tables"
                        ),
                        affected_table=table,
                        affected_columns=[column],
                        suggested_action=(
                            f"Search the codebase for references to `{reference}` "
                            "before dropping"
                        ),
                        context={"line_number": statement.line_number},
                    )
                )

        return issues

    def _find_query_references(
        self,
        queries: list[ExtractedQuery],
        source_file: str,
        statement_line_number: int,
        statement_sql: str,
        table_identifiers: set[str],
        column: str,
    ) -> list[ExtractedQuery]:
        statement_sql_normalized = " ".join(statement_sql.strip().lower().split())
        referenced: list[ExtractedQuery] = []
        for query in queries:
            if (
                query.source_file == source_file
                and query.line_number == statement_line_number
                and " ".join(query.sql.strip().lower().split())
                == statement_sql_normalized
            ):
                continue
            if _references_table_column(query.sql, table_identifiers, column):
                referenced.append(query)
        return referenced

    def _is_fk_target(
        self,
        queries: list[ExtractedQuery],
        table_identifiers: set[str],
        column: str,
    ) -> bool:
        for query in queries:
            for table_identifier in table_identifiers:
                references_re = re.compile(
                    _REFERENCES_RE_TEMPLATE.format(table=re.escape(table_identifier)),
                    re.IGNORECASE,
                )
                match = references_re.search(query.sql)
                if not match:
                    continue
                referenced_columns = {
                    _normalize_identifier(part)
                    for part in match.group("columns").split(",")
                }
                if column in referenced_columns:
                    return True
        return False
