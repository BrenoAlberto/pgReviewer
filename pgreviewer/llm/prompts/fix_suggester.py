"""LLM-powered exact fix suggestions for code pattern issues."""

from __future__ import annotations

import logging
from pathlib import Path

from pydantic import BaseModel

from pgreviewer.llm.client import LLMClient
from pgreviewer.llm.structured_output import generate_structured

logger = logging.getLogger(__name__)

OUTPUT_TOKENS = 800
# Context lines shown before/after the flagged range so the LLM understands
# the full pattern even when injections are spread across multiple statements.
_CONTEXT_LINES_BEFORE = 20
_CONTEXT_LINES_AFTER = 5

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
    replace_start_line: int
    replace_end_line: int


def _extract_lines(source: str, start_line: int, end_line: int) -> str:
    """Return lines start_line..end_line (1-based, inclusive)."""
    lines = source.splitlines()
    return "\n".join(lines[start_line - 1 : end_line])


def _build_prompt(
    source: str,
    file_path: str,
    start_line: int,
    end_line: int,
    detector_name: str,
    issue_description: str,
) -> str:
    lines = source.splitlines()
    total = len(lines)
    ctx_start = max(1, start_line - _CONTEXT_LINES_BEFORE)
    ctx_end = min(total, end_line + _CONTEXT_LINES_AFTER)
    numbered_parts = []
    for abs_i, line in enumerate(lines[ctx_start - 1 : ctx_end], start=ctx_start):
        marker = ">>>" if start_line <= abs_i <= end_line else "   "
        numbered_parts.append(f"{abs_i:4d} {marker} {line}")
    numbered = "\n".join(numbered_parts)
    return (
        "You are a Python code reviewer fixing a database issue.\n\n"
        f"**File:** `{file_path}` — lines {ctx_start}–{ctx_end} shown "
        f"(flagged range: {start_line}–{end_line}, marked with `>>>`):\n"
        "```\n"
        f"{numbered}\n"
        "```\n\n"
        f"**Issue (`{detector_name}`):** {issue_description}\n\n"
        "Produce a **complete, correct fix** for this issue. Rules:\n"
        "- For `sql_injection_fstring`: use SQLAlchemy `text()` with **bound "
        "parameters** (`:param_name` placeholders + a params dict). "
        "NEVER use string concatenation or f-strings for SQL values.\n"
        "- You may expand the replacement range beyond the flagged lines if the "
        "full fix requires changing earlier statements (e.g. where_clauses "
        "building).\n"
        "- Preserve the exact indentation of the original lines.\n"
        "- Do NOT include the function signature or code outside the replacement.\n"
        f"- `replace_start_line`: first line number of your replacement "
        f"(>= {ctx_start}, <= {start_line}).\n"
        f"- `replace_end_line`: last line number of your replacement "
        f"(>= {end_line}, <= {ctx_end}).\n"
        "- `fixed_code`: the replacement lines only, no markdown fences.\n"
        "- `one_line_explanation`: one sentence describing what changed."
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

    if not source.strip():
        return None

    llm_client = client
    try:
        if llm_client is None:
            llm_client = LLMClient()
    except Exception as exc:
        logger.debug("fix_suggester: LLM unavailable — %s", exc)
        return None

    prompt = _build_prompt(
        source=source,
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

        # Update context with the LLM's actual replacement range (may be wider
        # than the originally detected range, e.g. covering where_clauses too).
        issue.context["start_line"] = fix.replace_start_line
        issue.context["line_number"] = fix.replace_end_line

        # Use ```suggestion so GitHub renders an apply button.
        issue.suggested_action = (
            f"{fix.one_line_explanation}\n\n```suggestion\n{fix.fixed_code}\n```"
        )
        logger.info(
            "fix_suggester: enriched %s at %s L%s–%s",
            issue.detector_name,
            file_path,
            fix.replace_start_line,
            fix.replace_end_line,
        )
