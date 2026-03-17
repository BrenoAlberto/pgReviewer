from pgreviewer.analysis.migration_detectors import BaseMigrationDetector
from pgreviewer.config import settings
from pgreviewer.core.models import Issue, ParsedMigration, SchemaInfo, Severity


class LargeTableDDLDetector(BaseMigrationDetector):
    @property
    def name(self) -> str:
        return "large_table_ddl"

    def detect(self, migration: ParsedMigration, schema: SchemaInfo) -> list[Issue]:
        issues: list[Issue] = []

        for statement in migration.statements:
            table = statement.table
            if not table:
                # Try a fallback for DROP INDEX if table is missing
                table = self._find_table_for_statement(statement, schema)

            if not table:
                continue

            # Normalize table name for lookup
            clean_table = table.strip().strip('"')

            if clean_table in schema.tables:
                row_estimate = schema.tables[clean_table].row_estimate
                if row_estimate > settings.LARGE_TABLE_DDL_THRESHOLD:
                    issues.append(
                        Issue(
                            severity=Severity.WARNING,
                            detector_name=self.name,
                            description=(
                                f"DDL statement detected on large table "
                                f"'{clean_table}' ({row_estimate:,} estimated rows). "
                                "Even 'safe' operations can be risky on tables "
                                "of this size."
                            ),
                            affected_table=clean_table,
                            affected_columns=[],
                            suggested_action=(
                                "Review with your DBA. Consider running during "
                                "low-traffic hours or using online schema change "
                                "tools (e.g., pg_repack)."
                            ),
                            context={
                                "line_number": statement.line_number,
                                "row_estimate": row_estimate,
                            },
                        )
                    )

        return issues

    def _find_table_for_statement(self, statement, schema: SchemaInfo) -> str | None:
        """Heuristic fallback to find a table name for statements like DROP INDEX."""
        raw_sql = statement.raw_sql.upper()
        if "DROP INDEX" in raw_sql:
            # Extract index name
            import re

            match = re.search(
                r"DROP\s+INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+EXISTS\s+)?(?P<name>[^\s(;,]+)",
                statement.raw_sql,
                re.IGNORECASE,
            )
            if match:
                idx_name = match.group("name").strip().strip('"')
                # Look for this index in the schema
                for table_name, table_info in schema.tables.items():
                    for idx in table_info.indexes:
                        if idx.name == idx_name:
                            return table_name
        return None
