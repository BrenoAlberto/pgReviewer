import re
from pathlib import Path

from pgreviewer.parsing.file_classifier import FileType, classify_file
from pgreviewer.parsing.sql_extractor_migration import extract_from_alembic_file

REPO_ROOT = Path(__file__).resolve().parent.parent
DEMO_ROOT = REPO_ROOT / "demos" / "02-alembic"


def test_demo_02_alembic_001_is_classified_and_contains_fk_sql() -> None:
    migration = DEMO_ROOT / "alembic" / "versions" / "001_create_tables.py"
    source = migration.read_text(encoding="utf-8")

    assert (
        classify_file("alembic/versions/001_create_tables.py", source)
        == FileType.MIGRATION_PYTHON
    )

    extracted_sql = [query.sql for query in extract_from_alembic_file(migration)]
    assert any("ALTER TABLE events" in statement for statement in extracted_sql)
    assert any("FOREIGN KEY (account_id)" in statement for statement in extracted_sql)

    assert "op.create_index(\"ix_events_created_at\"" in source
    assert not re.search(
        r"op\.create_index\([^\\n]+postgresql_concurrently\s*=",
        source,
    )


def test_demo_02_alembic_fix_and_readme_contract() -> None:
    fix_migration = (
        DEMO_ROOT / "alembic" / "versions" / "002_add_indexes.py"
    ).read_text(encoding="utf-8")
    readme = (DEMO_ROOT / "README.md").read_text(encoding="utf-8")

    assert "postgresql_concurrently=True" in fix_migration

    assert "## Alembic-specific setup" in readme
    assert "add_foreign_key_without_index" in readme
    assert "create_index_not_concurrently" in readme
