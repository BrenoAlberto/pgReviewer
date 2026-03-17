import re

from pgreviewer.analysis.migration_detectors import BaseMigrationDetector
from pgreviewer.core.models import Issue, ParsedMigration, SchemaInfo, Severity

# 1. ADD COLUMN col TYPE REFERENCES reftable(refcol)
_ADD_COLUMN_FK_RE = re.compile(
    r"\bADD\s+COLUMN\s+(?P<column>(?:\"[^\"]+\"|[^\s]+))\s+[^\s]+\s+REFERENCES\s+(?P<reftable>(?:\"[^\"]+\"|[^\s(]+))",
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)

# 2. ADD CONSTRAINT name FOREIGN KEY (col) REFERENCES reftable(refcol)
_ADD_CONSTRAINT_FK_RE = re.compile(
    r"\bADD\s+CONSTRAINT\s+(?P<name>(?:\"[^\"]+\"|[^\s]+))\s+FOREIGN\s+KEY\s*\((?P<columns>[^)]+)\)\s+REFERENCES\s+(?P<reftable>(?:\"[^\"]+\"|[^\s(]+))",
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)

# 3. CREATE [UNIQUE] INDEX [CONCURRENTLY] name ON table (col, ...)
_CREATE_INDEX_RE = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?P<name>[^\s(]+)\s+ON\s+(?P<table>[^\s(]+)\s*\((?P<columns>[^)]+)\)",
    re.IGNORECASE | re.VERBOSE,
)


def _normalize_identifier(identifier: str) -> str:
    return identifier.strip().strip('"')


def _parse_columns(cols_str: str) -> list[str]:
    return [_normalize_identifier(c) for c in cols_str.split(",")]


class FKWithoutIndexDetector(BaseMigrationDetector):
    @property
    def name(self) -> str:
        return "add_foreign_key_without_index"

    def detect(self, migration: ParsedMigration, schema: SchemaInfo) -> list[Issue]:
        issues: list[Issue] = []

        # 1. Collect all indexes created in THIS migration
        newly_indexed: dict[str, list[list[str]]] = {}  # table -> list of column lists
        for stmt in migration.statements:
            if stmt.statement_type == "CREATE INDEX":
                match = _CREATE_INDEX_RE.search(stmt.raw_sql)
                if match:
                    table = _normalize_identifier(match.group("table"))
                    cols = _parse_columns(match.group("columns"))
                    newly_indexed.setdefault(table, []).append(cols)

        # 2. Check for FK additions
        for stmt in migration.statements:
            if stmt.statement_type != "ALTER TABLE":
                continue

            table = stmt.table
            if not table:
                continue
            table = _normalize_identifier(table)

            found_fks: list[tuple[list[str], int]] = []  # cols, line_number

            # Check ADD COLUMN ... REFERENCES
            for match in _ADD_COLUMN_FK_RE.finditer(stmt.raw_sql):
                col = _normalize_identifier(match.group("column"))
                found_fks.append(([col], stmt.line_number))

            # Check ADD CONSTRAINT ... FOREIGN KEY
            for match in _ADD_CONSTRAINT_FK_RE.finditer(stmt.raw_sql):
                cols = _parse_columns(match.group("columns"))
                found_fks.append((cols, stmt.line_number))

            for fk_cols, line in found_fks:
                if self._is_indexed(table, fk_cols, schema, newly_indexed):
                    continue

                issues.append(
                    Issue(
                        severity=Severity.CRITICAL,
                        detector_name=self.name,
                        description=(
                            f"Foreign key columns {fk_cols} on table '{table}' "
                            "are not indexed. This will cause sequential scans "
                            "during joins and ON DELETE actions."
                        ),
                        affected_table=table,
                        affected_columns=fk_cols,
                        suggested_action=(
                            f"Add an index on {fk_cols}. Suggested SQL: "
                            f"CREATE INDEX CONCURRENTLY idx_{table}_"
                            f"{'_'.join(fk_cols)} ON {table} "
                            f"({', '.join(fk_cols)});"
                        ),
                        context={"line_number": line},
                    )
                )

        return issues

    def _is_indexed(
        self,
        table: str,
        cols: list[str],
        schema: SchemaInfo,
        newly_indexed: dict[str, list[list[str]]],
    ) -> bool:
        """Check if columns are indexed either in existing schema or this migration."""
        # 1. Check if ANY index in THIS migration covers the FK columns (as a prefix)
        for idx_cols in newly_indexed.get(table, []):
            if self._cols_match(cols, idx_cols):
                return True

        # 2. Check existing schema
        if table in schema.tables:
            for idx in schema.tables[table].indexes:
                if self._cols_match(cols, idx.columns):
                    return True

        return False

    def _cols_match(self, fk_cols: list[str], idx_cols: list[str]) -> bool:
        """Indexes can cover multiple columns.
        They cover a FK if fk_cols is a prefix of idx_cols.
        """
        if len(fk_cols) > len(idx_cols):
            return False
        return all(fk_cols[i] == idx_cols[i] for i in range(len(fk_cols)))
