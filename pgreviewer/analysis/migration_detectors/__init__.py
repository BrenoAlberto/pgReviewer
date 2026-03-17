import importlib
import pkgutil
import re
from abc import ABC, abstractmethod

from pgreviewer.analysis.issue_detectors import DetectorRegistry
from pgreviewer.core.models import DDLStatement, Issue, ParsedMigration, SchemaInfo

_ALTER_TABLE_RE = re.compile(
    r"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?P<table>[^\s(]+)", re.IGNORECASE
)
_CREATE_INDEX_RE = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX(?:\s+CONCURRENTLY)?\s+[^\s]+\s+ON\s+(?P<table>[^\s(]+)",
    re.IGNORECASE,
)
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<table>[^\s(]+)", re.IGNORECASE
)
_DROP_TABLE_RE = re.compile(
    r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?P<table>[^\s(]+)", re.IGNORECASE
)


class BaseMigrationDetector(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def detect(self, migration: ParsedMigration, schema: SchemaInfo) -> list[Issue]:
        pass


def _load_all_submodules() -> None:
    package_name = __name__
    package = importlib.import_module(package_name)
    for _, module_name, _ in pkgutil.walk_packages(
        package.__path__, package_name + "."
    ):
        importlib.import_module(module_name)


def parse_ddl_statement(raw_sql: str, line_number: int) -> DDLStatement:
    normalized = " ".join(raw_sql.strip().split())
    upper = normalized.upper()
    table: str | None = None

    if upper.startswith("ALTER TABLE"):
        table_match = _ALTER_TABLE_RE.search(normalized)
        table = table_match.group("table") if table_match else None
        statement_type = (
            "DROP COLUMN" if " DROP COLUMN " in f" {upper} " else "ALTER TABLE"
        )
    elif upper.startswith("CREATE") and " INDEX " in upper:
        table_match = _CREATE_INDEX_RE.search(normalized)
        table = table_match.group("table") if table_match else None
        statement_type = "CREATE INDEX"
    elif upper.startswith("DROP TABLE"):
        table_match = _DROP_TABLE_RE.search(normalized)
        table = table_match.group("table") if table_match else None
        statement_type = "DROP TABLE"
    elif upper.startswith("CREATE TABLE"):
        table_match = _CREATE_TABLE_RE.search(normalized)
        table = table_match.group("table") if table_match else None
        statement_type = "CREATE TABLE"
    else:
        statement_type = " ".join(upper.split()[:2]) if upper else "UNKNOWN"

    return DDLStatement(
        statement_type=statement_type,
        table=table,
        raw_sql=raw_sql,
        line_number=line_number,
    )


def run_migration_detectors(
    migration: ParsedMigration,
    schema: SchemaInfo,
    disabled_detectors: list[str] | None = None,
) -> list[Issue]:
    _load_all_submodules()
    registry = DetectorRegistry(disabled_detectors=disabled_detectors)
    all_issues = []
    for detector in registry.migration_detectors():
        all_issues.extend(detector.detect(migration, schema))
    return all_issues
