from __future__ import annotations

from pathlib import Path

import pytest

from pgreviewer.llm.prompts.sql_extractor import ExtractedSQL, SQLExtractionResult
from pgreviewer.parsing import extraction_router
from pgreviewer.parsing.diff_parser import ChangedFile
from pgreviewer.parsing.file_classifier import FileType, classify_file

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "sql_patterns"


@pytest.mark.parametrize(
    ("fixture_name", "should_use_llm", "expected_count"),
    [
        ("simple_execute.py", False, 1),
        ("string_concat.py", True, 1),
        ("fstring_sql.py", True, 1),
        ("mixed_patterns.py", True, 2),
        ("no_sql.py", False, 0),
    ],
)
def test_extraction_routing_contract(
    fixture_name: str,
    should_use_llm: bool,
    expected_count: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = FIXTURE_DIR / fixture_name
    source = fixture_path.read_text(encoding="utf-8")

    raw_calls: list[str] = []
    llm_calls: list[str] = []

    original_extract_raw_sql = extraction_router.extract_raw_sql

    def _tracking_extract_raw_sql(python_source: str, file_path: str = ""):
        raw_calls.append(file_path)
        return original_extract_raw_sql(python_source, file_path)

    def _mock_extract_sql_with_llm(snippet: str, *, file_context: str, client=None):
        _ = snippet, client
        llm_calls.append(file_context)
        return _llm_result_for_fixture(Path(file_context).name)

    monkeypatch.setattr(extraction_router, "extract_raw_sql", _tracking_extract_raw_sql)
    monkeypatch.setattr(
        extraction_router,
        "extract_sql_with_llm",
        _mock_extract_sql_with_llm,
    )

    changed_file = _to_changed_file(fixture_path, source)
    file_type = classify_file(str(fixture_path), source)

    queries = extraction_router.route_extraction(changed_file, file_type)

    assert len(queries) == expected_count
    assert bool(llm_calls) is should_use_llm

    if fixture_name == "no_sql.py":
        assert file_type == FileType.IGNORE
        assert raw_calls == []
        assert llm_calls == []
    else:
        assert file_type == FileType.PYTHON_WITH_SQL
        assert raw_calls

    expected_sql = _expected_sql_annotations(fixture_path)
    actual_sql = {_normalize_sql(query.sql) for query in queries}
    for expected in expected_sql:
        assert _normalize_sql(expected) in actual_sql


def _to_changed_file(path: Path, source: str) -> ChangedFile:
    lines = source.splitlines()
    return ChangedFile(
        path=str(path),
        added_lines=lines,
        added_line_numbers=list(range(1, len(lines) + 1)),
    )


def _expected_sql_annotations(path: Path) -> list[str]:
    expected: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        marker = "# EXPECTED_SQL:"
        if not line.startswith(marker):
            continue
        value = line[len(marker) :].strip()
        if value:
            expected.append(value)
    return expected


def _llm_result_for_fixture(fixture_name: str) -> SQLExtractionResult:
    by_fixture: dict[str, SQLExtractionResult] = {
        "string_concat.py": SQLExtractionResult(
            queries=[
                ExtractedSQL(
                    sql="SELECT id, name FROM users WHERE status = 'active'",
                    confidence=0.84,
                    notes="string concatenation",
                )
            ]
        ),
        "fstring_sql.py": SQLExtractionResult(
            queries=[
                ExtractedSQL(
                    sql="SELECT * FROM users",
                    confidence=0.7,
                    notes="f-string template",
                )
            ]
        ),
        "mixed_patterns.py": SQLExtractionResult(
            queries=[
                ExtractedSQL(
                    sql="SELECT * FROM orders WHERE status = 'active'",
                    confidence=0.82,
                    notes="string concatenation",
                )
            ]
        ),
    }
    return by_fixture.get(fixture_name, SQLExtractionResult(queries=[]))


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.lower().split())
