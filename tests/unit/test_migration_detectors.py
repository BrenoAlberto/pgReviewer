from pathlib import Path

import pytest

from pgreviewer.analysis.migration_detectors import (
    parse_ddl_statement,
    run_migration_detectors,
)
from pgreviewer.core.models import ColumnInfo, ParsedMigration, SchemaInfo, TableInfo
from pgreviewer.parsing.sql_extractor_migration import split_sql_statements

FIXTURE_DIR = Path(__file__).parents[1] / "fixtures" / "migrations"


def _load_fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text()


def parse_migration(sql: str, source_file: str) -> ParsedMigration:
    extracted_queries = split_sql_statements(sql, file_path=source_file)
    statements = [
        parse_ddl_statement(query.sql, query.line_number) for query in extracted_queries
    ]
    return ParsedMigration(
        statements=statements,
        source_file=source_file,
        extracted_queries=extracted_queries,
    )


@pytest.fixture
def mock_schema() -> SchemaInfo:
    return SchemaInfo(
        tables={
            "orders": TableInfo(
                row_estimate=500_000,
                columns=[
                    ColumnInfo(name="legacy_id", type="bigint"),
                    ColumnInfo(name="archived_flag", type="boolean"),
                ],
            ),
            "users": TableInfo(
                row_estimate=100_000,
                columns=[
                    ColumnInfo(name="status", type="varchar(100)"),
                    ColumnInfo(name="name", type="varchar(100)"),
                ],
            ),
            "analytics_events": TableInfo(row_estimate=15_000_000),
            "small_orders": TableInfo(row_estimate=100),
        }
    )


@pytest.mark.parametrize(
    ("detector_name", "bad_fixture", "good_fixture"),
    [
        ("destructive_ddl", "destructive_ddl_bad.sql", "destructive_ddl_good.sql"),
        (
            "add_column_with_default",
            "add_column_default_bad.sql",
            "add_column_default_good.sql",
        ),
        (
            "alter_column_type",
            "alter_column_type_bad.sql",
            "alter_column_type_good.sql",
        ),
        (
            "drop_column_still_referenced",
            "drop_column_referenced_bad.sql",
            "drop_column_referenced_good.sql",
        ),
        (
            "add_foreign_key_without_index",
            "fk_without_index_bad.sql",
            "fk_without_index_good.sql",
        ),
        (
            "create_index_not_concurrently",
            "create_index_bad.sql",
            "create_index_good.sql",
        ),
        (
            "add_not_null_without_default",
            "not_null_without_default_bad.sql",
            "not_null_without_default_good.sql",
        ),
        ("large_table_ddl", "large_table_ddl_bad.sql", "large_table_ddl_good.sql"),
    ],
)
def test_detector_fixtures_cover_bad_and_good_cases(
    detector_name: str,
    bad_fixture: str,
    good_fixture: str,
    mock_schema: SchemaInfo,
):
    bad_sql = _load_fixture(bad_fixture)
    good_sql = _load_fixture(good_fixture)

    bad_migration = parse_migration(bad_sql, str(FIXTURE_DIR / bad_fixture))
    good_migration = parse_migration(good_sql, str(FIXTURE_DIR / good_fixture))

    bad_issues = [
        i
        for i in run_migration_detectors(bad_migration, mock_schema)
        if i.detector_name == detector_name
    ]
    good_issues = [
        i
        for i in run_migration_detectors(good_migration, mock_schema)
        if i.detector_name == detector_name
    ]

    assert len(bad_issues) >= 1
    assert len(good_issues) == 0
