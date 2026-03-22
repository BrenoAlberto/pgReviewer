"""Unit tests for ``pgr schema dump`` command and core schema_dumper module."""

from __future__ import annotations

import json
import subprocess
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from typer.testing import CliRunner

from pgreviewer.analysis.schema_dumper import (
    collect_all_stats,
    format_stats_comments,
    run_pg_dump,
)
from pgreviewer.cli.main import app
from pgreviewer.exceptions import SchemaDumpError

runner = CliRunner()


# ---------------------------------------------------------------------------
# run_pg_dump
# ---------------------------------------------------------------------------


_SUBPROCESS_PATCH = "pgreviewer.analysis.schema_dumper.subprocess.run"


class TestRunPgDump:
    def test_success(self):
        mock_result = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="CREATE TABLE orders ();\n", stderr=""
        )
        with patch(_SUBPROCESS_PATCH, return_value=mock_result):
            ddl = run_pg_dump("postgresql://u:p@localhost/db")
            assert "CREATE TABLE" in ddl

    def test_pg_dump_not_found(self):
        with (
            patch(_SUBPROCESS_PATCH, side_effect=FileNotFoundError),
            pytest.raises(SchemaDumpError, match="pg_dump not found"),
        ):
            run_pg_dump("postgresql://u:p@localhost/db")

    def test_pg_dump_error_exit(self):
        mock_result = subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr="connection refused"
        )
        with (
            patch(_SUBPROCESS_PATCH, return_value=mock_result),
            pytest.raises(SchemaDumpError, match="connection refused"),
        ):
            run_pg_dump("postgresql://u:p@localhost/db")


# ---------------------------------------------------------------------------
# collect_all_stats
# ---------------------------------------------------------------------------


def _record(**kwargs: object) -> MagicMock:
    """Create a dict-like mock record (matching asyncpg Record interface)."""
    rec = MagicMock()
    rec.__getitem__ = lambda self, key: kwargs[key]
    rec.__contains__ = lambda self, key: key in kwargs
    return rec


class TestCollectAllStats:
    @pytest.mark.asyncio
    async def test_collects_tables_indexes_columns(self):
        conn = AsyncMock()

        # Table stats
        conn.fetch.side_effect = [
            [_record(table_name="orders", row_estimate=50_000, size_bytes=4096000)],
            # Index definitions
            [
                _record(
                    index_name="ix_orders_user_id",
                    table_name="orders",
                    is_unique=False,
                    predicate=None,
                    index_type="btree",
                    columns=["user_id"],
                    include_columns=[],
                )
            ],
            # Column stats
            [
                _record(
                    table_name="orders",
                    column_name="user_id",
                    column_type="integer",
                    null_fraction=0.01,
                    distinct_count=500.0,
                )
            ],
        ]

        stats = await collect_all_stats(conn)

        assert "orders" in stats
        assert stats["orders"]["row_estimate"] == 50_000
        assert stats["orders"]["size_bytes"] == 4096000
        assert len(stats["orders"]["indexes"]) == 1
        assert stats["orders"]["indexes"][0]["name"] == "ix_orders_user_id"
        assert len(stats["orders"]["columns"]) == 1
        assert stats["orders"]["columns"][0]["name"] == "user_id"

    @pytest.mark.asyncio
    async def test_empty_database(self):
        conn = AsyncMock()
        conn.fetch.side_effect = [[], [], []]

        stats = await collect_all_stats(conn)
        assert stats == {}


# ---------------------------------------------------------------------------
# format_stats_comments
# ---------------------------------------------------------------------------


class TestFormatStatsComments:
    def test_single_table(self):
        stats = {
            "orders": {
                "row_estimate": 50000,
                "size_bytes": 4096000,
                "indexes": [],
                "columns": [],
            }
        }
        output = format_stats_comments(stats)

        assert "-- pgreviewer:meta" in output
        assert "-- pgreviewer:stats " in output

        # Extract and parse the JSON
        for line in output.splitlines():
            if line.startswith("-- pgreviewer:stats "):
                payload = json.loads(line.removeprefix("-- pgreviewer:stats "))
                assert "orders" in payload
                assert payload["orders"]["row_estimate"] == 50000
                break
        else:
            pytest.fail("No pgreviewer:stats line found")

    def test_multiple_tables_sorted(self):
        stats = {"zebra": {"row_estimate": 1}, "alpha": {"row_estimate": 2}}
        output = format_stats_comments(stats)
        lines = [
            line
            for line in output.splitlines()
            if line.startswith("-- pgreviewer:stats")
        ]
        assert len(lines) == 2
        # Should be sorted alphabetically
        assert '"alpha"' in lines[0]
        assert '"zebra"' in lines[1]

    def test_empty_stats(self):
        output = format_stats_comments({})
        assert "-- pgreviewer:meta" in output
        assert "pgreviewer:stats" not in output.replace("pgreviewer:meta", "")


# ---------------------------------------------------------------------------
# CLI integration (pgr schema dump)
# ---------------------------------------------------------------------------


class TestSchemaDumpCLI:
    def test_no_database_url_exits_1(self, monkeypatch):
        from pgreviewer.config import settings

        monkeypatch.setattr(settings, "DATABASE_URL", None)

        result = runner.invoke(app, ["schema", "dump"])
        assert result.exit_code == 1
        assert "DATABASE_URL" in result.output

    def test_successful_dump(self, tmp_path, monkeypatch):
        from pgreviewer.config import settings

        monkeypatch.setattr(settings, "DATABASE_URL", "postgresql://u:p@localhost/db")
        output_file = tmp_path / "schema.sql"

        mock_ddl = "CREATE TABLE orders (id serial);\n"
        mock_conn = AsyncMock()
        mock_conn.fetch.side_effect = [
            [_record(table_name="orders", row_estimate=100, size_bytes=8192)],
            [],
            [],
        ]

        with (
            patch(
                "pgreviewer.analysis.schema_dumper.run_pg_dump",
                return_value=mock_ddl,
            ),
            patch("asyncpg.connect", AsyncMock(return_value=mock_conn)),
        ):
            result = runner.invoke(
                app, ["schema", "dump", "--output", str(output_file)]
            )

        assert result.exit_code == 0, result.output
        assert output_file.exists()
        content = output_file.read_text()
        assert "CREATE TABLE" in content
        assert "pgreviewer:stats" in content

    def test_no_stats_flag(self, tmp_path, monkeypatch):
        from pgreviewer.config import settings

        monkeypatch.setattr(settings, "DATABASE_URL", "postgresql://u:p@localhost/db")
        output_file = tmp_path / "schema.sql"

        with patch(
            "pgreviewer.analysis.schema_dumper.run_pg_dump",
            return_value="CREATE TABLE t ();\n",
        ):
            result = runner.invoke(
                app,
                ["schema", "dump", "--output", str(output_file), "--no-stats"],
            )

        assert result.exit_code == 0, result.output
        content = output_file.read_text()
        assert "CREATE TABLE" in content
        assert "pgreviewer:stats" not in content
        assert "Stats collection skipped" in result.output

    def test_pg_dump_error_exits_1(self, monkeypatch):
        from pgreviewer.config import settings

        monkeypatch.setattr(settings, "DATABASE_URL", "postgresql://u:p@localhost/db")

        with patch(
            "pgreviewer.analysis.schema_dumper.run_pg_dump",
            side_effect=SchemaDumpError("pg_dump failed"),
        ):
            result = runner.invoke(app, ["schema", "dump", "--no-stats"])

        assert result.exit_code == 1
        assert "pg_dump failed" in result.output
