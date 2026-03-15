import json
from pathlib import Path

import pytest

from pgreviewer.analysis.issue_detectors.cartesian_join import CartesianJoinDetector
from pgreviewer.analysis.plan_parser import parse_explain
from pgreviewer.core.models import SchemaInfo, Severity

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "explain"


def _load_plan(fixture_name: str):
    with open(FIXTURE_DIR / fixture_name) as f:
        raw = json.load(f)
    return parse_explain(raw[0])


@pytest.fixture
def detector():
    return CartesianJoinDetector()


@pytest.fixture
def schema():
    return SchemaInfo()


def test_cartesian_join_critical(detector, schema):
    plan = _load_plan("cartesian_join.json")
    issues = detector.detect(plan, schema)

    assert len(issues) == 1
    issue = issues[0]
    assert issue.severity == Severity.CRITICAL
    assert "users" in issue.description or "users" in issue.context["tables"]
    assert "orders" in issue.description or "orders" in issue.context["tables"]


def test_proper_join_no_issue(detector, schema):
    plan = _load_plan("nested_loop.json")
    issues = detector.detect(plan, schema)

    assert len(issues) == 0
