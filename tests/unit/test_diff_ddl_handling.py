"""Tests that DDL statements in SQL migration files are handled gracefully.

When pgr diff processes a migration file containing DDL (CREATE TABLE,
ALTER TABLE, etc.), EXPLAIN cannot be run on those statements. The analyzer
must skip EXPLAIN for DDL and run only migration detectors instead.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from pgreviewer.core.models import ExtractedQuery


def _make_query(sql: str, line: int = 1) -> ExtractedQuery:
    return ExtractedQuery(
        sql=sql,
        source_file="db/migrations/0001.sql",
        line_number=line,
        extraction_method="migration_sql",
        confidence=1.0,
    )


@pytest.mark.asyncio
async def test_ddl_statements_skip_explain() -> None:
    """DDL statements must not call _analyse_query (which would try EXPLAIN)."""
    from pgreviewer.cli.commands.diff import _analyze_all_queries

    ddl = _make_query(
        "ALTER TABLE orders ADD CONSTRAINT fk"
        " FOREIGN KEY (user_id) REFERENCES users(id)"
    )

    with (
        patch(
            "pgreviewer.cli.commands.check._analyse_query",
            new_callable=AsyncMock,
        ) as mock_analyse,
        patch(
            "pgreviewer.core.backend.get_backend",
        ) as mock_backend,
    ):
        mock_backend.return_value.get_slow_queries = AsyncMock(return_value=[])
        results = await _analyze_all_queries([ddl], only_critical=False)

    mock_analyse.assert_not_called()
    assert len(results) == 1


@pytest.mark.asyncio
async def test_dml_statements_call_explain() -> None:
    """DML statements (SELECT etc.) must still go through _analyse_query."""
    from pgreviewer.cli.commands.diff import _analyze_all_queries
    from pgreviewer.core.degradation import AnalysisResult

    dml = _make_query("SELECT * FROM orders WHERE user_id = 1")

    fake_result = AnalysisResult()

    with (
        patch(
            "pgreviewer.cli.commands.check._analyse_query",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_analyse,
        patch(
            "pgreviewer.core.backend.get_backend",
        ) as mock_backend,
    ):
        mock_backend.return_value.get_slow_queries = AsyncMock(return_value=[])
        results = await _analyze_all_queries([dml], only_critical=False)

    mock_analyse.assert_called_once_with(dml.sql)
    assert len(results) == 1


@pytest.mark.asyncio
async def test_fk_without_index_detected_for_alter_table_ddl() -> None:
    """ALTER TABLE ADD CONSTRAINT FK without an index → WARNING in degraded-static mode.

    Without schema data (empty SchemaInfo), the detector cannot verify existing
    indexes and degrades to WARNING severity.
    """
    from pgreviewer.cli.commands.diff import _analyze_all_queries

    ddl = _make_query(
        "ALTER TABLE orders ADD CONSTRAINT fk_user"
        " FOREIGN KEY (user_id) REFERENCES users(id)"
    )

    with patch("pgreviewer.core.backend.get_backend") as mock_backend:
        mock_backend.return_value.get_slow_queries = AsyncMock(return_value=[])
        results = await _analyze_all_queries([ddl], only_critical=False)

    issues = results[0]["issues"]
    fk_issues = [
        i for i in issues if i.detector_name == "add_foreign_key_without_index"
    ]
    assert fk_issues, f"Expected add_foreign_key_without_index finding, got: {issues}"
    # Degraded-static mode: WARNING because schema is empty (no DB)
    assert all(i.severity.value == "WARNING" for i in fk_issues), (
        f"Expected WARNING in degraded mode, got: {[i.severity for i in fk_issues]}"
    )


@pytest.mark.asyncio
async def test_fk_with_index_in_same_file_suppresses_critical() -> None:
    """CREATE INDEX in the same file as ALTER TABLE FK must suppress the CRITICAL."""
    from pgreviewer.cli.commands.diff import _analyze_all_queries

    fk = _make_query(
        "ALTER TABLE orders ADD CONSTRAINT fk_user"
        " FOREIGN KEY (user_id) REFERENCES users(id)",
        line=10,
    )
    idx = _make_query(
        "CREATE INDEX CONCURRENTLY idx_orders_user_id ON orders (user_id)",
        line=20,
    )

    with patch("pgreviewer.core.backend.get_backend") as mock_backend:
        mock_backend.return_value.get_slow_queries = AsyncMock(return_value=[])
        results = await _analyze_all_queries([fk, idx], only_critical=False)

    fk_result = results[0]
    assert not any(
        i.detector_name == "add_foreign_key_without_index" for i in fk_result["issues"]
    ), (
        "Expected no add_foreign_key_without_index issue when CREATE INDEX "
        f"is present in the same file, got: {fk_result['issues']}"
    )
