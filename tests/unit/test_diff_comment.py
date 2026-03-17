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
    assert "add_foreign_key_without_index" in body
    assert "db/migrations/0001.sql" in body


def test_comment_extracts_sql_fix() -> None:
    body = format_diff_comment(_SAMPLE_DATA, now=_FIXED_TS)
    assert "CREATE INDEX CONCURRENTLY idx_orders_user_id" in body
    assert "Copy-ready fixes" in body


def test_comment_shows_skipped_details() -> None:
    body = format_diff_comment(_SAMPLE_DATA, now=_FIXED_TS)
    assert "some/ignored.py" in body
    assert "Analysis scope" in body


def test_comment_contains_timestamp() -> None:
    body = format_diff_comment(_SAMPLE_DATA, now=_FIXED_TS)
    assert "2026-03-17 10:25 UTC" in body


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
