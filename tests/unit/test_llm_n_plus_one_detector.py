from __future__ import annotations

import json

import pytest

from pgreviewer.analysis.code_pattern_detectors.base import ParsedFile
from pgreviewer.analysis.code_pattern_detectors.llm_n_plus_one import (
    LLMNPlusOneAnalyzer,
)
from pgreviewer.core.models import Severity
from pgreviewer.exceptions import BudgetExceededError
from pgreviewer.parsing.treesitter import TSParser


def _parsed_python_file(path: str, source: str) -> ParsedFile:
    parser = TSParser("python")
    return ParsedFile(
        path=path,
        tree=parser.parse_file(source, language="python"),
        language="python",
        content=source,
    )


class _FakeClient:
    def __init__(self, response_text: str | Exception):
        self.response_text = response_text
        self.calls: list[dict[str, object]] = []

    def generate(self, prompt: str, category: str, estimated_tokens: int) -> str:
        self.calls.append(
            {
                "prompt": prompt,
                "category": category,
                "estimated_tokens": estimated_tokens,
            }
        )
        if isinstance(self.response_text, Exception):
            raise self.response_text
        return self.response_text


def test_should_analyze_uses_db_hint_keywords() -> None:
    assert LLMNPlusOneAnalyzer.should_analyze("process_order", "service.fetch_order()")
    assert not LLMNPlusOneAnalyzer.should_analyze(
        "format_name", "service.format_name()"
    )


@pytest.mark.parametrize(
    ("confidence", "expected_severity"),
    [
        (0.95, Severity.CRITICAL),
        (0.85, Severity.WARNING),
        (0.70, Severity.INFO),
    ],
)
def test_analyze_uncertain_call_parses_response_and_maps_severity(
    confidence: float, expected_severity: Severity
) -> None:
    loop_file = _parsed_python_file(
        "app/loop.py",
        "def run(service, orders):\n"
        "    for order in orders:\n"
        "        service.process_order(order.id)\n",
    )
    helper_file = _parsed_python_file(
        "app/helpers.py",
        "def process_order(order_id):\n    return repo.fetch_order(order_id)\n",
    )
    fake_client = _FakeClient(
        json.dumps(
            {
                "is_n_plus_one": True,
                "confidence": confidence,
                "explanation": "Repeated query in loop.",
                "suggested_fix": "Batch IDs into a single query.",
            }
        )
    )
    analyzer = LLMNPlusOneAnalyzer(client=fake_client)

    issue = analyzer.analyze_uncertain_call(
        files=[loop_file, helper_file],
        loop_file=loop_file,
        loop_line=3,
        function_name="process_order",
        call_text="service.process_order",
    )

    assert issue is not None
    assert issue.severity == expected_severity
    assert issue.detector_name == "llm_n_plus_one"
    assert issue.confidence == confidence
    assert issue.suggested_action == "Batch IDs into a single query."
    assert issue.context["called_function_file"] == "app/helpers.py"
    assert fake_client.calls[0]["category"] == "classification"
    assert "<loop_file path='app/loop.py'>" in fake_client.calls[0]["prompt"]
    assert (
        "<called_function_file path='app/helpers.py'>" in fake_client.calls[0]["prompt"]
    )


def test_analyze_uncertain_call_returns_info_issue_when_budget_is_exceeded() -> None:
    loop_file = _parsed_python_file(
        "app/loop.py",
        "def run(service, orders):\n"
        "    for order in orders:\n"
        "        service.process_order(order.id)\n",
    )
    helper_file = _parsed_python_file(
        "app/helpers.py",
        "def process_order(order_id):\n    return repo.fetch_order(order_id)\n",
    )
    analyzer = LLMNPlusOneAnalyzer(client=_FakeClient(BudgetExceededError("budget")))

    issue = analyzer.analyze_uncertain_call(
        files=[loop_file, helper_file],
        loop_file=loop_file,
        loop_line=3,
        function_name="process_order",
        call_text="service.process_order",
    )

    assert issue is not None
    assert issue.severity == Severity.INFO
    assert "unresolved — LLM budget exceeded" in issue.description
    assert issue.context["reason"] == "llm budget exceeded"
