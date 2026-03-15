import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pgreviewer.analysis.issue_detectors.sequential_scan import SequentialScanDetector
from pgreviewer.analysis.plan_parser import parse_explain
from pgreviewer.core.models import SchemaInfo, Severity

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "explain"


def _load_plan(fixture_name: str):
    with open(FIXTURE_DIR / fixture_name) as f:
        raw = json.load(f)
    return parse_explain(raw[0])


@pytest.fixture
def detector():
    return SequentialScanDetector()


@pytest.fixture
def schema():
    return SchemaInfo()


def test_large_table_seq_scan_is_critical(detector, schema):
    """Seq scan on a 1.5M-row table must produce a CRITICAL issue."""
    plan = _load_plan("seq_scan_large.json")
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    issue = issues[0]
    assert issue.severity == Severity.CRITICAL
    assert issue.detector_name == "sequential_scan"
    assert issue.affected_table == "events"
    assert issue.context["estimated_rows"] == 1_500_000
    assert "index" in issue.suggested_action.lower()


def test_small_lookup_table_produces_no_issue(detector, schema):
    """Seq scan on a 100-row lookup table must not raise any issue."""
    plan = _load_plan("seq_scan_small.json")
    issues = detector.detect(plan, schema)

    assert issues == []


def test_seq_scan_no_filter_produces_warning_issue(detector, schema):
    """Seq scan with no filter on a 15K-row table (above threshold, below 1M)
    must produce a WARNING-severity issue with a full-scan review suggestion."""
    plan = _load_plan("seq_scan_no_filter.json")
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    issue = issues[0]
    assert issue.severity == Severity.WARNING
    assert issue.affected_table == "config"
    assert issue.context["estimated_rows"] == 15_000
    assert "full table scan" in issue.suggested_action.lower()


def test_existing_seq_scan_fixture_produces_warning_issue(detector, schema):
    """The existing seq_scan.json fixture (100K rows) must produce a WARNING issue."""
    plan = _load_plan("seq_scan.json")
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    issue = issues[0]
    assert issue.severity == Severity.WARNING
    assert issue.affected_table == "users"
    assert issue.context["estimated_rows"] == 100_000


def test_threshold_respected(detector, schema):
    """Rows exactly at the threshold must not raise an issue."""
    plan = _load_plan("seq_scan_no_filter.json")
    with patch(
        "pgreviewer.analysis.issue_detectors.sequential_scan.settings"
    ) as mock_settings:
        mock_settings.SEQ_SCAN_ROW_THRESHOLD = 15_000
        issues = detector.detect(plan, schema)

    assert issues == []


def test_index_scan_not_flagged(detector, schema):
    """An index scan must never be flagged by the sequential scan detector."""
    plan = _load_plan("index_scan.json")
    issues = detector.detect(plan, schema)

    assert issues == []
