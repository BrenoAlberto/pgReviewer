from __future__ import annotations

import json
from typing import TYPE_CHECKING

from pydantic import BaseModel

from pgreviewer.llm.client import LLMClient
from pgreviewer.llm.structured_output import generate_structured

if TYPE_CHECKING:
    from pgreviewer.core.models import Issue

OUTPUT_TOKENS = 300


class ReportSummary(BaseModel):
    summary: str
    confidence: float


def build_report_summarizer_prompt(
    issues: list[Issue],
    llm_interpretation: dict[str, object] | None = None,
) -> str:
    findings = {
        "issue_count": len(issues),
        "issues": [
            {
                "severity": issue.severity.value,
                "detector_name": issue.detector_name,
                "description": issue.description,
                "suggested_action": issue.suggested_action,
                "affected_table": issue.affected_table,
                "affected_columns": issue.affected_columns,
                "confidence": issue.confidence,
                "context": issue.context or {},
            }
            for issue in issues
        ],
        "llm_interpretation": llm_interpretation,
    }
    findings_json = json.dumps(findings, indent=2, sort_keys=True)
    return (
        "You are summarizing a PostgreSQL performance analysis report for engineers.\n"
        "Using only the structured findings, produce a specific "
        "business-impact summary.\n"
        "Requirements:\n"
        "- Write 2-3 sentences.\n"
        "- Keep the summary at or below 150 words.\n"
        "- Name the concrete tables/columns and expected impact when available "
        "(cost, latency, row counts, scan type, or request-level impact).\n"
        "- Avoid generic statements.\n\n"
        "<structured_findings>\n"
        f"{findings_json}\n"
        "</structured_findings>"
    )


def summarize_report(
    issues: list[Issue],
    llm_interpretation: dict[str, object] | None = None,
    *,
    client: LLMClient | None = None,
) -> ReportSummary:
    llm_client = client or LLMClient()
    prompt = build_report_summarizer_prompt(issues, llm_interpretation)
    return generate_structured(
        client=llm_client,
        prompt=prompt,
        response_model=ReportSummary,
        category="reporting",
        estimated_tokens=OUTPUT_TOKENS,
    )
