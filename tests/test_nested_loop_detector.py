import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pgreviewer.analysis.issue_detectors.nested_loop import NestedLoopLargeOuterDetector
from pgreviewer.analysis.plan_parser import parse_explain
from pgreviewer.core.models import IssueSeverity, SchemaInfo

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "explain"


def _load_plan(fixture_name: str):
    with open(FIXTURE_DIR / fixture_name) as f:
        raw = json.load(f)
    return parse_explain(raw[0])


@pytest.fixture
def detector():
    return NestedLoopLargeOuterDetector()


@pytest.fixture
def schema():
    return SchemaInfo()


def test_large_outer_critical(detector, schema):
    """Nested loop with 500K outer rows (> 100K) must produce a CRITICAL issue."""
    plan = _load_plan("nested_loop.json")
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    issue = issues[0]
    assert issue.severity == IssueSeverity.CRITICAL
    assert issue.detector_name == "nested_loop_large_outer"
    assert issue.context["outer_table"] == "orders"
    assert issue.context["outer_rows"] == 500_000
    assert "hash join" in issue.context["suggested_action"].lower()


def test_large_outer_warning(detector, schema):
    """Nested loop with 50K outer rows (between 1K and 100K) must produce a HIGH issue."""
    plan = _load_plan("nested_loop_warning.json")
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    issue = issues[0]
    assert issue.severity == IssueSeverity.HIGH
    assert issue.detector_name == "nested_loop_large_outer"
    assert issue.context["outer_table"] == "orders"
    assert issue.context["outer_rows"] == 50_000
    assert "hash join" in issue.context["suggested_action"].lower()


def test_small_outer_no_issue(detector, schema):
    """Nested loop with 50 outer rows (below threshold) must not produce any issue."""
    plan = _load_plan("nested_loop_small.json")
    issues = detector.detect(plan, schema)

    assert issues == []


def test_threshold_respected(detector, schema):
    """Outer rows equal to the threshold must not raise an issue.

    Detection uses ``outer.plan_rows <= threshold``, so a value exactly at
    the threshold is intentionally excluded.
    """
    plan = _load_plan("nested_loop_warning.json")
    with patch(
        "pgreviewer.analysis.issue_detectors.nested_loop.settings"
    ) as mock_settings:
        mock_settings.NESTED_LOOP_OUTER_THRESHOLD = 50_000
        issues = detector.detect(plan, schema)

    assert issues == []


def test_inner_table_included_in_context(detector, schema):
    """The inner table name must be captured in the issue context."""
    plan = _load_plan("nested_loop_warning.json")
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    assert issues[0].context["inner_table"] == "users"
