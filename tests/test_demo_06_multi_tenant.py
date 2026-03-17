from pathlib import Path

from pgreviewer.config import load_pgreviewer_config
from pgreviewer.parsing.file_classifier import FileType, classify_file

REPO_ROOT = Path(__file__).resolve().parent.parent
DEMO_ROOT = REPO_ROOT / "demos" / "06-multi-tenant"


def test_demo_06_schema_queries_and_fix_contract() -> None:
    schema_sql = (DEMO_ROOT / "migrations" / "0001_schema.sql").read_text(
        encoding="utf-8"
    )
    query_sql = (DEMO_ROOT / "queries" / "tenant_queries.sql").read_text(
        encoding="utf-8"
    )
    fix_sql = (
        DEMO_ROOT / "migrations" / "0002_fix_composite_indexes.sql"
    ).read_text(encoding="utf-8")

    assert (
        classify_file("migrations/0001_schema.sql", schema_sql)
        == FileType.MIGRATION_SQL
    )
    assert classify_file("queries/tenant_queries.sql", query_sql) == FileType.RAW_SQL

    assert "CREATE INDEX idx_events_user_id ON events (user_id)" in schema_sql
    assert "tenant_id" in query_sql and "user_id" in query_sql

    assert "DROP INDEX IF EXISTS idx_events_user_id" in fix_sql
    assert (
        "CREATE INDEX idx_events_tenant_user_id ON events (tenant_id, user_id)"
        in fix_sql
    )
    assert (
        "CREATE INDEX idx_events_tenant_created_at ON events "
        "(tenant_id, created_at DESC)"
        in fix_sql
    )


def test_demo_06_readme_and_config_contract() -> None:
    config = load_pgreviewer_config(DEMO_ROOT / ".pgreviewer.yml")
    readme = (DEMO_ROOT / "README.md").read_text(encoding="utf-8")

    assert config.rules["sequential_scan"].severity == "critical"
    assert config.rules["missing_index_on_filter"].severity == "warning"
    assert config.thresholds.seq_scan_rows == 1

    assert "tenant_id" in readme and "user_id" in readme
    assert "missing_index_on_filter" in readme
    assert "sequential_scan" in readme
    assert "Multi-tenant index design pattern" in readme
