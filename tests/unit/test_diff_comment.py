from datetime import UTC, datetime

from pgreviewer.reporting.diff_comment import format_diff_comment
from pgreviewer.reporting.pr_comment import REPORT_SIGNATURE

_FIXED_TS = datetime(2026, 3, 17, 10, 25, tzinfo=UTC)

_SAMPLE_DATA = {
    "skipped": [{"file": "some/ignored.py", "reason": "Ignored by classifier"}],
    "results": [
        {
            "source_file": "db/migrations/0001.sql",
            "line_number": 38,
            "overall_severity": "CRITICAL",
            "issues": [
                {
                    "severity": "CRITICAL",
                    "detector_name": "add_foreign_key_without_index",
                    "description": (
                        "FK columns ['user_id'] on table 'orders' are not indexed."
                    ),
                    "affected_table": "orders",
                    "affected_columns": ["user_id"],
                    "suggested_action": (
                        "Add an index on ['user_id']. Suggested SQL: "
                        "CREATE INDEX CONCURRENTLY"
                        " idx_orders_user_id ON orders (user_id);"
                    ),
                }
            ],
            "recommendations": [],
        }
    ],
    "model_diffs": [],
    "cross_cutting_findings": [],
}


def test_comment_starts_with_signature() -> None:
    body = format_diff_comment(_SAMPLE_DATA, now=_FIXED_TS)
    assert body.startswith(REPORT_SIGNATURE)


def test_comment_contains_logo_link() -> None:
    body = format_diff_comment(_SAMPLE_DATA, now=_FIXED_TS)
    assert "logo.svg" in body


def test_comment_shows_critical_badge() -> None:
    body = format_diff_comment(_SAMPLE_DATA, now=_FIXED_TS)
    assert "CRITICAL" in body


def test_comment_contains_issues_table() -> None:
    body = format_diff_comment(_SAMPLE_DATA, now=_FIXED_TS)
    # Detector rendered as friendly title from _DETECTOR_CONTEXT
    assert "Foreign key column missing an index" in body
    assert "db/migrations/0001.sql" in body


def test_comment_extracts_sql_fix() -> None:
    body = format_diff_comment(_SAMPLE_DATA, now=_FIXED_TS)
    assert "CREATE INDEX CONCURRENTLY idx_orders_user_id" in body


def test_comment_shows_skipped_details() -> None:
    body = format_diff_comment(_SAMPLE_DATA, now=_FIXED_TS)
    assert "some/ignored.py" in body
    assert "skipped" in body


def test_comment_contains_timestamp() -> None:
    body = format_diff_comment(_SAMPLE_DATA, now=_FIXED_TS)
    assert "2026-03-17 10:25 UTC" in body


def test_code_pattern_issues_appear_in_comment() -> None:
    data = {
        "skipped": [],
        "results": [],
        "model_diffs": [],
        "cross_cutting_findings": [],
        "code_pattern_issues": [
            {
                "severity": "CRITICAL",
                "detector_name": "query_in_loop",
                "description": "db.query(Task) called inside a for-loop",
                "suggested_action": "Use a single batched query",
                "source_file": "app/routers/standup.py",
                "line_number": 14,
            }
        ],
    }
    body = format_diff_comment(data, now=_FIXED_TS)
    assert "CRITICAL" in body
    assert "N+1 query pattern" in body
    assert "standup.py" in body


def test_pass_comment_shows_no_issues_message() -> None:
    data = {
        "skipped": [],
        "results": [],
        "model_diffs": [],
        "cross_cutting_findings": [],
    }
    body = format_diff_comment(data, now=_FIXED_TS)
    assert "No issues found" in body
    assert "PASS" in body


# ---------------------------------------------------------------------------
# Analysis tier (#303)
# ---------------------------------------------------------------------------

_BASE_DATA: dict = {
    "skipped": [],
    "results": [],
    "model_diffs": [],
    "cross_cutting_findings": [],
}


def test_tier_static_shows_static_label() -> None:
    data = {
        **_BASE_DATA,
        "metadata": {"analysis_mode": "static_only", "schema_used": False},
    }
    body = format_diff_comment(data, now=_FIXED_TS)
    assert "🔍" in body
    assert "Static Analysis" in body


def test_tier_schema_aware_shows_schema_label() -> None:
    data = {
        **_BASE_DATA,
        "metadata": {"analysis_mode": "static_only", "schema_used": True},
    }
    body = format_diff_comment(data, now=_FIXED_TS)
    assert "📊" in body
    assert "Schema-Aware Analysis" in body


def test_tier_full_shows_full_label() -> None:
    data = {**_BASE_DATA, "metadata": {"analysis_mode": "full", "schema_used": False}}
    body = format_diff_comment(data, now=_FIXED_TS)
    assert "⚡" in body
    assert "Full Analysis" in body


# ---------------------------------------------------------------------------
# Scope line (#303)
# ---------------------------------------------------------------------------


def test_scope_line_shows_migration_counts() -> None:
    data = {
        **_BASE_DATA,
        "metadata": {"file_type_counts": {"MIGRATION_PYTHON": 3, "MIGRATION_SQL": 1}},
    }
    body = format_diff_comment(data, now=_FIXED_TS)
    assert "Analyzed:" in body
    assert "3 Python migrations" in body
    assert "1 SQL migration" in body


def test_scope_line_absent_when_no_file_type_counts() -> None:
    data = {**_BASE_DATA, "metadata": {}}
    body = format_diff_comment(data, now=_FIXED_TS)
    assert "Analyzed:" not in body


# ---------------------------------------------------------------------------
# Collapsible severity sections (#303)
# ---------------------------------------------------------------------------


def _data_with_issue(severity: str) -> dict:
    return {
        "skipped": [],
        "results": [
            {
                "source_file": "db/0001.sql",
                "line_number": 5,
                "overall_severity": severity,
                "issues": [
                    {
                        "severity": severity,
                        "detector_name": "create_index_not_concurrently",
                        "description": "Test issue",
                        "suggested_action": "Fix it",
                    }
                ],
                "recommendations": [],
            }
        ],
        "model_diffs": [],
        "cross_cutting_findings": [],
    }


def test_critical_section_is_open_by_default() -> None:
    body = format_diff_comment(_data_with_issue("CRITICAL"), now=_FIXED_TS)
    assert "<details open>" in body


def test_warning_section_is_collapsed_by_default() -> None:
    body = format_diff_comment(_data_with_issue("WARNING"), now=_FIXED_TS)
    assert "<details>" in body
    assert "<details open>" not in body


def test_severity_section_header_shows_count() -> None:
    body = format_diff_comment(_data_with_issue("WARNING"), now=_FIXED_TS)
    assert "Warning — 1 finding" in body


# ---------------------------------------------------------------------------
# Upgrade prompt (#303)
# ---------------------------------------------------------------------------


def test_upgrade_prompt_shown_for_static_tier() -> None:
    data = {**_BASE_DATA, "metadata": {"schema_used": False}}
    body = format_diff_comment(data, now=_FIXED_TS)
    assert "pgr schema dump" in body
    assert ".pgreviewer/schema.sql" in body


def test_upgrade_prompt_shown_for_schema_aware_tier() -> None:
    data = {**_BASE_DATA, "metadata": {"schema_used": True}}
    body = format_diff_comment(data, now=_FIXED_TS)
    assert "EXPLAIN-based analysis" in body


def test_no_upgrade_prompt_for_full_tier() -> None:
    data = {**_BASE_DATA, "metadata": {"analysis_mode": "full"}}
    body = format_diff_comment(data, now=_FIXED_TS)
    assert "Want" not in body


# ---------------------------------------------------------------------------
# pgPilot nudge (#303 + ADR #321)
# ---------------------------------------------------------------------------


def test_pgpilot_nudge_shown_for_static_tier() -> None:
    data = {**_BASE_DATA, "metadata": {"schema_used": False}}
    body = format_diff_comment(data, now=_FIXED_TS)
    assert "pgPilot" in body
    assert "Enable schema-aware analysis" in body


def test_pgpilot_nudge_absent_for_schema_aware_tier() -> None:
    data = {**_BASE_DATA, "metadata": {"schema_used": True}}
    body = format_diff_comment(data, now=_FIXED_TS)
    assert "Enable schema-aware analysis" not in body


def test_pgpilot_nudge_absent_for_full_tier() -> None:
    data = {**_BASE_DATA, "metadata": {"analysis_mode": "full"}}
    body = format_diff_comment(data, now=_FIXED_TS)
    assert "Enable schema-aware analysis" not in body
