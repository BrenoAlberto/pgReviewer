from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pgreviewer.llm.prompts.sql_extractor import ExtractedSQL, SQLExtractionResult
from pgreviewer.parsing.diff_parser import ChangedFile
from pgreviewer.parsing.extraction_router import route_extraction
from pgreviewer.parsing.file_classifier import FileType

if TYPE_CHECKING:
    from _pytest.logging import LogCaptureFixture


def _fixture_path(name: str) -> str:
    return str(
        Path(__file__).resolve().parent.parent / "fixtures" / "python_sources" / name
    )


def test_route_extraction_skips_llm_when_ast_is_high_confidence(
    monkeypatch, caplog: LogCaptureFixture
):
    file = ChangedFile(
        path=_fixture_path("simple_execute.py"),
        added_lines=['cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))'],
        added_line_numbers=[5],
    )

    def _fail_if_called(*args, **kwargs):
        raise AssertionError("LLM should not be called for high-confidence AST output")

    monkeypatch.setattr(
        "pgreviewer.parsing.extraction_router.extract_sql_with_llm", _fail_if_called
    )

    with caplog.at_level("INFO"):
        queries = route_extraction(file, FileType.PYTHON_WITH_SQL)

    assert len(queries) == 1
    assert queries[0].confidence >= 0.85
    assert "AST: 1 queries (high confidence) — LLM not needed" in caplog.text


def test_route_extraction_calls_llm_when_ast_has_low_confidence(monkeypatch):
    file = ChangedFile(
        path=_fixture_path("concatenated_sql.py"),
        added_lines=['sql = "SELECT * FROM users WHERE status = " + status'],
        added_line_numbers=[3],
    )
    calls: list[str] = []

    def _mock_llm(snippet: str, *, file_context: str, client=None):
        calls.append(snippet)
        return SQLExtractionResult(
            queries=[
                ExtractedSQL(
                    sql="SELECT * FROM users WHERE status = $1",
                    confidence=0.8,
                    notes="dynamic WHERE clause",
                )
            ]
        )

    monkeypatch.setattr(
        "pgreviewer.parsing.extraction_router.extract_sql_with_llm", _mock_llm
    )
    queries = route_extraction(file, FileType.PYTHON_WITH_SQL)

    assert len(calls) == 1
    assert any(query.extraction_method == "llm" for query in queries)


def test_route_extraction_calls_llm_when_ast_returns_nothing_for_sql_like_region(
    monkeypatch,
):
    file = ChangedFile(
        path=_fixture_path("simple_execute.py"),
        added_lines=["# SELECT id FROM users"],
        added_line_numbers=[1],
    )

    monkeypatch.setattr(
        "pgreviewer.parsing.extraction_router.extract_raw_sql",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        "pgreviewer.parsing.extraction_router.extract_sql_with_llm",
        lambda *args, **kwargs: SQLExtractionResult(
            queries=[
                ExtractedSQL(
                    sql="SELECT id FROM users",
                    confidence=0.7,
                    notes="comment looked SQL-like",
                )
            ]
        ),
    )

    queries = route_extraction(file, FileType.PYTHON_WITH_SQL)
    assert len(queries) == 1
    assert queries[0].extraction_method == "llm"


def test_route_extraction_deduplicates_similar_sql_between_ast_and_llm(
    monkeypatch, tmp_path: Path
):
    source = """
def run(cursor, table_name):
    cursor.execute("SELECT id FROM users")
    sql = "SELECT * FROM " + table_name
    cursor.execute(sql)
""".strip()
    file_path = tmp_path / "mixed_queries.py"
    file_path.write_text(source, encoding="utf-8")
    file = ChangedFile(
        path=str(file_path),
        added_lines=source.splitlines(),
        added_line_numbers=[1, 2, 3, 4],
    )

    monkeypatch.setattr(
        "pgreviewer.parsing.extraction_router.extract_sql_with_llm",
        lambda *args, **kwargs: SQLExtractionResult(
            queries=[
                ExtractedSQL(
                    sql="SELECT id FROM users",
                    confidence=0.95,
                    notes="duplicate of AST",
                ),
                ExtractedSQL(
                    sql="SELECT * FROM users",
                    confidence=0.9,
                    notes="dynamic table resolution",
                ),
            ]
        ),
    )

    queries = route_extraction(file, FileType.PYTHON_WITH_SQL)
    sqls = [query.sql for query in queries]
    assert sqls.count("SELECT id FROM users") == 1
    assert "SELECT * FROM users" in sqls
