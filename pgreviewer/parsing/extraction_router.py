from __future__ import annotations

import logging
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import TYPE_CHECKING

from pgreviewer.llm.prompts.sql_extractor import (
    extract_sql_with_llm,
    map_to_extracted_queries,
)
from pgreviewer.parsing.file_classifier import FileType
from pgreviewer.parsing.sql_extractor_migration import (
    extract_from_alembic_file,
    extract_from_sql_file,
)
from pgreviewer.parsing.sql_extractor_raw import extract_raw_sql

if TYPE_CHECKING:
    from pgreviewer.core.models import ExtractedQuery
    from pgreviewer.parsing.diff_parser import ChangedFile

logger = logging.getLogger(__name__)

_CONFIDENCE_THRESHOLD = 0.85
_SQL_LIKE_RE = re.compile(r"\b(SELECT|INSERT|UPDATE|DELETE)\b", re.IGNORECASE)


def route_extraction(
    file: ChangedFile, classified_type: FileType
) -> list[ExtractedQuery]:
    path = Path(file.path)
    if classified_type in (FileType.MIGRATION_SQL, FileType.RAW_SQL):
        return extract_from_sql_file(path)
    if classified_type == FileType.MIGRATION_PYTHON:
        return extract_from_alembic_file(path)
    if classified_type != FileType.PYTHON_WITH_SQL:
        return []

    source = path.read_text(encoding="utf-8")
    ast_queries = extract_raw_sql(source, file_path=file.path)
    ast_high = [q for q in ast_queries if q.confidence >= _CONFIDENCE_THRESHOLD]
    ast_low = [q for q in ast_queries if q.confidence < _CONFIDENCE_THRESHOLD]

    if ast_queries and not ast_low:
        logger.info("AST: %s queries (high confidence) — LLM not needed", len(ast_high))
        return ast_queries

    llm_queries: list[ExtractedQuery] = []
    if ast_low:
        for query in ast_low:
            snippet = _line_window(source, query.line_number)
            llm_queries.extend(_extract_with_llm(file.path, snippet, query.line_number))
    elif _looks_sql_like(file, source):
        snippet = _sql_like_region(file, source)
        line_number = file.added_line_numbers[0] if file.added_line_numbers else 1
        llm_queries.extend(_extract_with_llm(file.path, snippet, line_number))

    return _deduplicate_by_similarity([*ast_high, *llm_queries])


def _extract_with_llm(
    file_path: str,
    snippet: str,
    line_number: int,
) -> list[ExtractedQuery]:
    if not snippet.strip():
        return []
    result = extract_sql_with_llm(snippet, file_context=file_path)
    return map_to_extracted_queries(
        result,
        source_file=file_path,
        line_number=line_number,
    )


def _looks_sql_like(file: ChangedFile, source: str) -> bool:
    region = "\n".join(file.added_lines).strip() or source
    return bool(_SQL_LIKE_RE.search(region))


def _sql_like_region(file: ChangedFile, source: str) -> str:
    if not file.added_lines:
        return source
    sql_like_lines = [line for line in file.added_lines if _SQL_LIKE_RE.search(line)]
    return "\n".join(sql_like_lines) if sql_like_lines else "\n".join(file.added_lines)


def _line_window(source: str, line_number: int, radius: int = 3) -> str:
    lines = source.splitlines()
    if not lines:
        return ""
    start = max(1, line_number - radius)
    end = min(len(lines), line_number + radius)
    return "\n".join(lines[start - 1 : end])


def _deduplicate_by_similarity(
    queries: list[ExtractedQuery], threshold: float = 0.97
) -> list[ExtractedQuery]:
    deduped: list[ExtractedQuery] = []
    normalized_sql: list[str] = []
    for query in queries:
        candidate = _normalize_sql(query.sql)
        if any(
            candidate == existing
            or SequenceMatcher(None, candidate, existing).ratio() >= threshold
            for existing in normalized_sql
        ):
            continue
        deduped.append(query)
        normalized_sql.append(candidate)
    return deduped


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.lower().split())
