"""Correlate extracted SQL queries against workload (pg_stat_statements) queries."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Literal

from pgreviewer.core.models import ExtractedQuery, SlowQuery

logger = logging.getLogger(__name__)

_STRING_LITERAL_RE = re.compile(r"'(?:''|[^'])*'")
_NUMERIC_LITERAL_RE = re.compile(r"(?<!\$)\b\d+(?:\.\d+)?\b")
_NOW_LITERAL_RE = re.compile(r"\bnow\s*\(\s*\)", re.IGNORECASE)
_PLACEHOLDER_RE = re.compile(r"\$\d+")
_TOKEN_RE = re.compile(r"\$\d+|[a-z_][a-z0-9_]*", re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")
_FUZZY_JACCARD_THRESHOLD = 0.85


@dataclass(frozen=True)
class WorkloadMatch:
    extracted_query: ExtractedQuery
    slow_query: SlowQuery
    similarity_score: float
    match_type: Literal["exact", "fuzzy"]


def _canonicalize_placeholders(sql: str) -> str:
    mapping: dict[str, str] = {}
    counter = 1

    def _replace(match: re.Match[str]) -> str:
        nonlocal counter
        placeholder = match.group(0)
        if placeholder not in mapping:
            mapping[placeholder] = f"${counter}"
            counter += 1
        return mapping[placeholder]

    return _PLACEHOLDER_RE.sub(_replace, sql)


def _normalize_for_matching(sql: str) -> str:
    normalized = sql.strip().rstrip(";")
    normalized = _STRING_LITERAL_RE.sub("$1", normalized)
    normalized = _NUMERIC_LITERAL_RE.sub("$1", normalized)
    normalized = _NOW_LITERAL_RE.sub("$1", normalized)
    normalized = _canonicalize_placeholders(normalized)
    normalized = _WHITESPACE_RE.sub(" ", normalized)
    return normalized.lower()


def _token_jaccard_similarity(left: str, right: str) -> float:
    left_tokens = {
        token.lower()
        for token in _TOKEN_RE.findall(left)
        if not (len(token) == 1 and token.isalpha())
    }
    right_tokens = {
        token.lower()
        for token in _TOKEN_RE.findall(right)
        if not (len(token) == 1 and token.isalpha())
    }
    if not left_tokens and not right_tokens:
        return 1.0
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def correlate(
    extracted_queries: list[ExtractedQuery], slow_queries: list[SlowQuery]
) -> list[WorkloadMatch]:
    if not extracted_queries or not slow_queries:
        return []

    normalized_slow_queries = [
        (slow_query, _normalize_for_matching(slow_query.query_text))
        for slow_query in slow_queries
    ]
    matched_slow_query_ids: set[int] = set()
    matches: list[WorkloadMatch] = []

    for extracted_query in extracted_queries:
        normalized_extracted = _normalize_for_matching(extracted_query.sql)
        exact_match = next(
            (
                slow_query
                for slow_query, normalized_slow in normalized_slow_queries
                if normalized_slow == normalized_extracted
            ),
            None,
        )
        if exact_match is not None:
            matched_slow_query_ids.add(id(exact_match))
            matches.append(
                WorkloadMatch(
                    extracted_query=extracted_query,
                    slow_query=exact_match,
                    similarity_score=1.0,
                    match_type="exact",
                )
            )
            continue

        best_slow_query: SlowQuery | None = None
        best_similarity = 0.0
        for slow_query, normalized_slow in normalized_slow_queries:
            similarity = _token_jaccard_similarity(normalized_extracted, normalized_slow)
            if similarity > best_similarity:
                best_similarity = similarity
                best_slow_query = slow_query

        if best_slow_query is not None and best_similarity >= _FUZZY_JACCARD_THRESHOLD:
            matched_slow_query_ids.add(id(best_slow_query))
            matches.append(
                WorkloadMatch(
                    extracted_query=extracted_query,
                    slow_query=best_slow_query,
                    similarity_score=best_similarity,
                    match_type="fuzzy",
                )
            )

    for slow_query in slow_queries:
        if id(slow_query) not in matched_slow_query_ids:
            logger.debug("Unmatched slow query: %s", slow_query.query_text)

    return matches
