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


def _extract_code_window(source: str, line_number: int, radius: int = 20) -> str:
    """Return the enclosing function or a ±radius-line window around line_number."""
    lines = source.splitlines()
    if not lines:
        return ""

    # Walk backwards from line_number to find the enclosing def/async def
    zero = line_number - 1
    func_start = zero
    for i in range(zero, -1, -1):
        stripped = lines[i].lstrip()
        if stripped.startswith("def ") or stripped.startswith("async def "):
            func_start = i
            break

    # Walk forward to find the end of the function (first line with equal or
    # lesser indentation after the def, or end of file)
    def_indent = len(lines[func_start]) - len(lines[func_start].lstrip())
    func_end = min(len(lines) - 1, func_start + 1)
    for i in range(func_start + 1, len(lines)):
        line = lines[i]
        if line.strip() == "":
            func_end = i
            continue
        indent = len(line) - len(line.lstrip())
        if indent <= def_indent and line.strip():
            func_end = i - 1
            break
        func_end = i

    # Fall back to ±radius if the function is huge (>60 lines)
    if func_end - func_start > 60:
        func_start = max(0, zero - radius)
        func_end = min(len(lines) - 1, zero + 5)

    return "\n".join(lines[func_start : func_end + 1])


def _build_prompt(
    code_window: str,
    file_path: str,
    line_number: int,
    detector_name: str,
    issue_description: str,
) -> str:
    return (
        "You are a Python code reviewer fixing a database issue.\n\n"
        f"**File:** `{file_path}`  **Line:** {line_number}\n"
        f"**Issue (`{detector_name}`):** {issue_description}\n\n"
        "**Affected code:**\n"
        "```python\n"
        f"{code_window}\n"
        "```\n\n"
        "Rewrite the affected function to fix the issue. Rules:\n"
        "- Preserve the exact function signature and all imports already in scope\n"
        "- Keep the same indentation style\n"
        "- Fix ONLY the identified problem — do not refactor unrelated code\n"
        "- `fixed_code`: the complete corrected function body (no markdown fences)\n"
        "- `one_line_explanation`: one sentence explaining what you changed and why"
    )


def suggest_exact_fix(
    *,
    file_path: str,
    line_number: int,
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

    code_window = _extract_code_window(source, line_number)
    if not code_window.strip():
        return None

    llm_client = client
    try:
        if llm_client is None:
            llm_client = LLMClient()
    except Exception as exc:
        logger.debug("fix_suggester: LLM unavailable — %s", exc)
        return None

    prompt = _build_prompt(
        code_window=code_window,
        file_path=file_path,
        line_number=line_number,
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
        line_number = ctx.get("line_number")
        if not file_path or not line_number:
            continue
        if issue.detector_name not in _SUPPORTED_DETECTORS:
            continue

        fix = suggest_exact_fix(
            file_path=file_path,
            line_number=line_number,
            detector_name=issue.detector_name,
            issue_description=issue.description,
            client=llm_client,
        )
        if fix is None:
            continue

        issue.suggested_action = (
            f"{fix.one_line_explanation}\n\n```python\n{fix.fixed_code}\n```"
        )
        logger.info(
            "fix_suggester: enriched %s at %s:%s",
            issue.detector_name,
            file_path,
            line_number,
        )
