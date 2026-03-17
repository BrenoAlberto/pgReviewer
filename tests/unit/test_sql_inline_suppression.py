from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from pgreviewer.analysis.issue_detectors import run_all_detectors
from pgreviewer.analysis.plan_parser import parse_explain
from pgreviewer.core.models import SchemaInfo
from pgreviewer.parsing.suppression_parser import parse_inline_suppressions

if TYPE_CHECKING:
    import pytest

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "explain"


def _load_plan(fixture_name: str):
    with open(FIXTURE_DIR / fixture_name) as fixture:
        raw = json.load(fixture)
    return parse_explain(raw[0])


def test_parse_inline_suppressions_ignore_all() -> None:
    suppression = parse_inline_suppressions(
        "SELECT * FROM events -- pgreviewer:ignore",
        known_rules={"sequential_scan", "high_cost"},
    )

    assert suppression.suppress_all is True
    assert suppression.rules == set()
    assert suppression.unknown_rules == set()


def test_parse_inline_suppressions_with_known_and_unknown_rules() -> None:
    suppression = parse_inline_suppressions(
        "SELECT 1 -- pgreviewer:ignore[sequential_scan, typo_rule]",
        known_rules={"sequential_scan", "high_cost"},
    )

    assert suppression.suppress_all is False
    assert suppression.rules == {"sequential_scan"}
    assert suppression.unknown_rules == {"typo_rule"}


def test_run_all_detectors_suppresses_all_issues_with_inline_ignore() -> None:
    plan = _load_plan("seq_scan_large.json")
    suppression_stats: dict[str, int] = {}

    issues = run_all_detectors(
        plan,
        SchemaInfo(),
        source_sql="SELECT * FROM events -- pgreviewer:ignore",
        suppression_stats=suppression_stats,
    )

    assert issues == []
    assert suppression_stats["suppressed_issues"] == 3


def test_run_all_detectors_suppresses_only_named_rule() -> None:
    plan = _load_plan("seq_scan_large.json")
    suppression_stats: dict[str, int] = {}

    issues = run_all_detectors(
        plan,
        SchemaInfo(),
        source_sql="SELECT * FROM events -- pgreviewer:ignore[sequential_scan]",
        suppression_stats=suppression_stats,
    )
    detector_names = {issue.detector_name for issue in issues}

    assert "sequential_scan" not in detector_names
    assert "high_cost" in detector_names
    assert suppression_stats["suppressed_issues"] == 1


def test_unknown_rule_name_emits_warning(caplog: pytest.LogCaptureFixture) -> None:
    plan = _load_plan("seq_scan_large.json")
    caplog.set_level(logging.WARNING)

    run_all_detectors(
        plan,
        SchemaInfo(),
        source_sql="SELECT * FROM events -- pgreviewer:ignore[typo_rule_name]",
    )

    assert "Unknown rule 'typo_rule_name' in pgreviewer:ignore comment" in caplog.text
