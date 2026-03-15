import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pgreviewer.analysis.issue_detectors.missing_index_on_filter import (
    MissingIndexOnFilterDetector,
    _extract_filter_columns,
)
from pgreviewer.analysis.plan_parser import parse_explain
from pgreviewer.core.models import IndexInfo, IssueSeverity, SchemaInfo, TableInfo

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "explain"


def _load_plan(fixture_name: str):
    with open(FIXTURE_DIR / fixture_name) as f:
        raw = json.load(f)
    return parse_explain(raw[0])


@pytest.fixture
def detector():
    return MissingIndexOnFilterDetector()


@pytest.fixture
def schema_no_indexes():
    """Schema with no indexes defined."""
    return SchemaInfo()


@pytest.fixture
def schema_with_user_id_index():
    """Schema that already has an index on orders(user_id)."""
    return SchemaInfo(
        tables={
            "orders": TableInfo(
                indexes=[
                    IndexInfo(name="idx_orders_user_id", columns=["user_id"]),
                ]
            ),
        }
    )


# ---------------------------------------------------------------------------
# Column extraction helper
# ---------------------------------------------------------------------------


def test_extract_single_equality_column():
    assert _extract_filter_columns("(user_id = $1)") == ["user_id"]


def test_extract_multiple_columns():
    cols = _extract_filter_columns("((user_id = $1) AND (status = 'active'::text))")
    assert "user_id" in cols
    assert "status" in cols


def test_extract_ignores_sql_keywords():
    # 'is' and 'null' are SQL keywords, not column names
    cols = _extract_filter_columns("(deleted_at IS NULL)")
    assert "is" not in cols
    assert "null" not in cols


# ---------------------------------------------------------------------------
# Detector: missing index scenario
# ---------------------------------------------------------------------------


def test_missing_index_emits_issue(detector, schema_no_indexes):
    """Seq scan on 'orders' with user_id filter and no index must emit an issue."""
    plan = _load_plan("seq_scan_orders_filter.json")
    issues = detector.detect(plan, schema_no_indexes)

    assert len(issues) == 1
    issue = issues[0]
    assert issue.detector_name == "missing_index_on_filter"
    assert issue.severity == IssueSeverity.MEDIUM
    assert issue.context["affected_table"] == "orders"
    assert "user_id" in issue.context["suggested_columns"]
    assert "user_id" in issue.context["suggested_action"]


# ---------------------------------------------------------------------------
# Detector: existing index suppresses issue
# ---------------------------------------------------------------------------


def test_existing_index_suppresses_issue(detector, schema_with_user_id_index):
    """When idx_orders_user_id already exists no issue must be emitted."""
    plan = _load_plan("seq_scan_orders_filter.json")
    issues = detector.detect(plan, schema_with_user_id_index)

    assert issues == []


# ---------------------------------------------------------------------------
# Detector: no filter → no issue
# ---------------------------------------------------------------------------


def test_seq_scan_without_filter_emits_no_issue(detector, schema_no_indexes):
    """A Seq Scan without a Filter expression must not trigger this detector."""
    plan = _load_plan("seq_scan.json")
    issues = detector.detect(plan, schema_no_indexes)

    assert issues == []


# ---------------------------------------------------------------------------
# Detector: index scan → no issue
# ---------------------------------------------------------------------------


def test_index_scan_not_flagged(detector, schema_no_indexes):
    """An Index Scan must never be flagged by this detector."""
    plan = _load_plan("index_scan.json")
    issues = detector.detect(plan, schema_no_indexes)

    assert issues == []


# ---------------------------------------------------------------------------
# Detector: ignored table → no issue
# ---------------------------------------------------------------------------


def test_ignored_table_emits_no_issue(detector, schema_no_indexes):
    """Tables in settings.IGNORE_TABLES must be silently skipped."""
    plan = _load_plan("seq_scan_orders_filter.json")
    with patch(
        "pgreviewer.analysis.issue_detectors.missing_index_on_filter.settings"
    ) as mock_settings:
        mock_settings.IGNORE_TABLES = ["orders"]
        issues = detector.detect(plan, schema_no_indexes)

    assert issues == []
