"""LLM-powered exact fix suggestions for code pattern issues."""

from __future__ import annotations

import logging
from pathlib import Path

from pydantic import BaseModel

from pgreviewer.llm.client import LLMClient
from pgreviewer.llm.structured_output import generate_structured

logger = logging.getLogger(__name__)

OUTPUT_TOKENS = 800
# Only generate exact fixes for these detectors — ones where the LLM can
# meaningfully rewrite the affected code rather than just pointing at config.
_SUPPORTED_DETECTORS = frozenset(
    {
        "sql_injection_fstring",
        "query_in_loop",
        "sqlalchemy_n_plus_one",
    }
)


class ExactFix(BaseModel):
    fixed_code: str
    one_line_explanation: str


def _extract_lines(source: str, start_line: int, end_line: int) -> str:
    """Return lines start_line..end_line (1-based, inclusive)."""
    lines = source.splitlines()
    return "\n".join(lines[start_line - 1 : end_line])


def _build_prompt(
    bad_lines: str,
    file_path: str,
    start_line: int,
    end_line: int,
    detector_name: str,
    issue_description: str,
) -> str:
    return (
        "You are a Python code reviewer fixing a database issue.\n\n"
        f"**File:** `{file_path}`  "
        f"**Lines {start_line}–{end_line} to replace:**\n"
        "```python\n"
        f"{bad_lines}\n"
        "```\n\n"
        f"**Issue (`{detector_name}`):** {issue_description}\n\n"
        "Return the corrected replacement for exactly those lines. Rules:\n"
        "- Preserve the exact indentation of the original lines\n"
        "- Do NOT include the function signature or surrounding code\n"
        "- Fix ONLY the identified problem\n"
        "- `fixed_code`: the replacement lines only, no markdown fences\n"
        "- `one_line_explanation`: one sentence describing what changed"
    )


def suggest_exact_fix(
    *,
    file_path: str,
    start_line: int,
    end_line: int,
    detector_name: str,
    issue_description: str,
    client: LLMClient | None = None,
) -> ExactFix | None:
    """Return an LLM-generated exact fix for the issue, or None if unavailable."""
    if detector_name not in _SUPPORTED_DETECTORS:
        return None

    try:
        source = Path(file_path).read_text(encoding="utf-8")
    except OSError:
        logger.debug("fix_suggester: cannot read %s", file_path)
        return None

    bad_lines = _extract_lines(source, start_line, end_line)
    if not bad_lines.strip():
        return None

    llm_client = client
    try:
        if llm_client is None:
            llm_client = LLMClient()
    except Exception as exc:
        logger.debug("fix_suggester: LLM unavailable — %s", exc)
        return None

    prompt = _build_prompt(
        bad_lines=bad_lines,
        file_path=file_path,
        start_line=start_line,
        end_line=end_line,
        detector_name=detector_name,
        issue_description=issue_description,
    )

    try:
        return generate_structured(
            client=llm_client,
            prompt=prompt,
            response_model=ExactFix,
            category="fix_suggestion",
            estimated_tokens=OUTPUT_TOKENS,
        )
    except Exception as exc:
        logger.debug("fix_suggester: generation failed — %s", exc)
        return None


def enrich_with_exact_fixes(issues: list, client: LLMClient | None = None) -> None:
    """Mutate each Issue's suggested_action with an LLM-generated exact fix.

    Only enriches issues that have both ``file`` and ``line_number`` in their
    context and whose detector is in ``_SUPPORTED_DETECTORS``.  Failures are
    logged at DEBUG and silently skipped — the generic template remains.
    """
    try:
        llm_client = client or LLMClient()
    except Exception as exc:
        logger.debug("fix_suggester: LLM unavailable, skipping enrichment — %s", exc)
        return

    for issue in issues:
        ctx = issue.context or {}
        file_path = ctx.get("file")
        end_line = ctx.get("line_number")
        start_line = ctx.get("start_line") or end_line
        if not file_path or not end_line:
            continue
        if issue.detector_name not in _SUPPORTED_DETECTORS:
            continue

        fix = suggest_exact_fix(
            file_path=file_path,
            start_line=start_line,
            end_line=end_line,
            detector_name=issue.detector_name,
            issue_description=issue.description,
            client=llm_client,
        )
        if fix is None:
            continue

        # Use ```suggestion so GitHub renders an apply button.
        # start_line/end_line are already in context for comment_manager.
        issue.suggested_action = (
            f"{fix.one_line_explanation}\n\n```suggestion\n{fix.fixed_code}\n```"
        )
        logger.info(
            "fix_suggester: enriched %s at %s L%s–%s",
            issue.detector_name,
            file_path,
            start_line,
            end_line,
        )
