from pgreviewer.analysis.migration_detectors import BaseMigrationDetector
from pgreviewer.core.models import Issue, ParsedMigration, SchemaInfo, Severity


class DestructiveDDLDetector(BaseMigrationDetector):
    @property
    def name(self) -> str:
        return "destructive_ddl"

    def detect(self, migration: ParsedMigration, schema: SchemaInfo) -> list[Issue]:
        del schema

        issues: list[Issue] = []
        for statement in migration.statements:
            if statement.statement_type == "DROP TABLE":
                issues.append(
                    Issue(
                        severity=Severity.CRITICAL,
                        detector_name=self.name,
                        description=f"DROP TABLE detected in {migration.source_file}",
                        affected_table=statement.table,
                        affected_columns=[],
                        suggested_action=(
                            "Validate rollback strategy and data retention plan."
                        ),
                        context={"line_number": statement.line_number},
                    )
                )
            elif statement.statement_type == "DROP COLUMN":
                issues.append(
                    Issue(
                        severity=Severity.CRITICAL,
                        detector_name=self.name,
                        description=f"DROP COLUMN detected in {migration.source_file}",
                        affected_table=statement.table,
                        affected_columns=[],
                        suggested_action=(
                            "Backfill/backup data and verify safe deploy order."
                        ),
                        context={"line_number": statement.line_number},
                    )
                )
        return issues
