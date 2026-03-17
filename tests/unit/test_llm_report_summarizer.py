from __future__ import annotations

from unittest.mock import Mock, patch

from pgreviewer.core.models import Issue, Severity
from pgreviewer.llm.prompts.report_summarizer import (
    ReportSummary,
    build_report_summarizer_prompt,
    summarize_report,
)


def _issue() -> Issue:
    return Issue(
        severity=Severity.WARNING,
        detector_name="high_cost",
        description="orders to users join scans orders sequentially",
        affected_table="orders",
        affected_columns=["user_id"],
        suggested_action="CREATE INDEX ON orders(user_id);",
        context={"estimated_rows": 2_000_000, "estimated_latency_ms": 3500},
    )


def test_build_report_summarizer_prompt_contains_constraints_and_findings() -> None:
    prompt = build_report_summarizer_prompt([_issue()], {"root_cause": "missing index"})

    assert "Write 2-3 sentences." in prompt
    assert "at or below 150 words" in prompt
    assert "<structured_findings>" in prompt
    assert "orders" in prompt
    assert "missing index" in prompt


def test_summarize_report_uses_structured_output_with_reporting_category() -> None:
    client = Mock()
    expected = ReportSummary(summary="targeted summary", confidence=0.92)

    with patch(
        "pgreviewer.llm.prompts.report_summarizer.generate_structured",
        return_value=expected,
    ) as generate:
        output = summarize_report(
            [_issue()], {"summary": "from llm analysis"}, client=client
        )

    assert output == expected
    generate.assert_called_once()
    kwargs = generate.call_args.kwargs
    assert kwargs["client"] is client
    assert kwargs["response_model"] is ReportSummary
    assert kwargs["category"] == "reporting"
