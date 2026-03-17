from __future__ import annotations

import logging

from pgreviewer.analysis.workload_correlator import correlate
from pgreviewer.core.models import ExtractedQuery, SlowQuery


def test_correlate_matches_literal_query_against_pg_stat_statements() -> None:
    extracted_query = ExtractedQuery(
        sql="SELECT * FROM orders WHERE user_id = 42",
        source_file="app/orders.py",
        line_number=10,
        extraction_method="ast",
        confidence=1.0,
    )
    slow_query = SlowQuery(
        query_text="SELECT * FROM orders WHERE user_id = $1",
        calls=100,
        mean_exec_time_ms=12.3,
        total_exec_time_ms=1234.0,
        rows=100,
    )

    matches = correlate([extracted_query], [slow_query])

    assert len(matches) == 1
    assert matches[0].extracted_query is extracted_query
    assert matches[0].slow_query is slow_query
    assert matches[0].match_type == "exact"
    assert matches[0].similarity_score == 1.0


def test_correlate_uses_fuzzy_jaccard_threshold_for_reasonable_variants() -> None:
    extracted_query = ExtractedQuery(
        sql="SELECT o.id FROM orders o WHERE o.user_id = 42",
        source_file="app/orders.py",
        line_number=10,
        extraction_method="ast",
        confidence=1.0,
    )
    slow_query = SlowQuery(
        query_text="SELECT id FROM orders WHERE user_id = $1",
        calls=100,
        mean_exec_time_ms=12.3,
        total_exec_time_ms=1234.0,
        rows=100,
    )

    matches = correlate([extracted_query], [slow_query])

    assert len(matches) == 1
    assert matches[0].match_type == "fuzzy"
    assert matches[0].similarity_score >= 0.85


def test_correlate_logs_unmatched_slow_queries_at_debug(caplog) -> None:
    extracted_query = ExtractedQuery(
        sql="SELECT * FROM users WHERE id = 1",
        source_file="app/users.py",
        line_number=2,
        extraction_method="ast",
        confidence=1.0,
    )
    slow_query = SlowQuery(
        query_text="SELECT * FROM invoices WHERE id = $1",
        calls=5,
        mean_exec_time_ms=1.0,
        total_exec_time_ms=5.0,
        rows=5,
    )

    with caplog.at_level(logging.DEBUG):
        matches = correlate([extracted_query], [slow_query])

    assert matches == []
    assert "Unmatched slow query: SELECT * FROM invoices WHERE id = $1" in caplog.text


def test_correlate_does_not_fuzzy_match_same_slow_query_twice() -> None:
    extracted_queries = [
        ExtractedQuery(
            sql="SELECT o.id FROM orders o WHERE o.user_id = 42",
            source_file="app/orders.py",
            line_number=10,
            extraction_method="ast",
            confidence=1.0,
        ),
        ExtractedQuery(
            sql="SELECT t.id FROM orders t WHERE t.user_id = 100",
            source_file="app/orders.py",
            line_number=20,
            extraction_method="ast",
            confidence=1.0,
        ),
    ]
    slow_query = SlowQuery(
        query_text="SELECT id FROM orders WHERE user_id = $1",
        calls=100,
        mean_exec_time_ms=12.3,
        total_exec_time_ms=1234.0,
        rows=100,
    )

    matches = correlate(extracted_queries, [slow_query])

    assert len(matches) == 1
