from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from pgreviewer.core.models import Issue, Severity
from pgreviewer.exceptions import (
    BudgetExceededError,
    LLMUnavailableError,
    StructuredOutputError,
)
from pgreviewer.llm.client import LLMClient
from pgreviewer.llm.structured_output import generate_structured

if TYPE_CHECKING:
    from pgreviewer.analysis.code_pattern_detectors.base import ParsedFile

logger = logging.getLogger(__name__)
_DB_HINT_KEYWORDS = ("get", "fetch", "load", "find", "query", "select")


def _response_model() -> type[BaseModel]:
    class NPlusOneClassification(BaseModel):
        is_n_plus_one: bool
        confidence: float = Field(ge=0.0, le=1.0)
        explanation: str
        suggested_fix: str

    return NPlusOneClassification


def _iter_nodes(root):
    stack = [root]
    while stack:
        node = stack.pop()
        yield node
        stack.extend(reversed(node.children))


def _confidence_to_severity(confidence: float) -> Severity:
    if confidence > 0.9:
        return Severity.CRITICAL
    if confidence > 0.8:
        return Severity.WARNING
    return Severity.INFO


class LLMNPlusOneAnalyzer:
    def __init__(self, client: LLMClient | None = None) -> None:
        self._client = client

    @staticmethod
    def should_analyze(function_name: str, context_text: str) -> bool:
        candidate = f"{function_name} {context_text}".lower()
        return any(keyword in candidate for keyword in _DB_HINT_KEYWORDS)

    @staticmethod
    def _find_function_definition(
        files: list[ParsedFile], *, function_name: str, exclude_path: str
    ) -> tuple[ParsedFile, int] | None:
        for parsed_file in files:
            if parsed_file.language != "python" or parsed_file.path == exclude_path:
                continue
            for node in _iter_nodes(parsed_file.tree.root_node):
                if node.type != "function_definition":
                    continue
                name_node = node.child_by_field_name("name")
                if name_node is None:
                    continue
                if name_node.text.decode("utf-8") == function_name:
                    return parsed_file, node.start_point[0] + 1
        return None

    @staticmethod
    def _estimate_tokens(loop_file_content: str, other_file_content: str) -> int:
        estimated = (len(loop_file_content) + len(other_file_content)) // 4
        return max(800, min(estimated, 4000))

    @staticmethod
    def _prompt(
        *,
        loop_file_path: str,
        loop_line: int,
        function_name: str,
        other_file_path: str,
        function_line: int,
        loop_file_content: str,
        other_file_content: str,
    ) -> str:
        return (
            "Does this loop result in repeated database queries?\n"
            f"The loop at {loop_file_path}:{loop_line} calls {function_name} "
            f"defined at {other_file_path}:{function_line}.\n"
            "Analyze both files and determine:\n"
            f"1. Does {function_name} execute a database query? (yes/no/uncertain)\n"
            "2. If yes, is there a way to batch this? Suggest a fix.\n"
            "Return JSON: "
            "{is_n_plus_one: bool, confidence: float, explanation: str, "
            "suggested_fix: str}\n\n"
            f"<loop_file path='{loop_file_path}'>\n"
            f"{loop_file_content}\n"
            "</loop_file>\n\n"
            f"<called_function_file path='{other_file_path}'>\n"
            f"{other_file_content}\n"
            "</called_function_file>"
        )

    def analyze_uncertain_call(
        self,
        *,
        files: list[ParsedFile],
        loop_file: ParsedFile,
        loop_line: int,
        function_name: str,
        call_text: str,
    ) -> Issue | None:
        definition = self._find_function_definition(
            files, function_name=function_name, exclude_path=loop_file.path
        )
        if definition is None:
            return None
        function_file, function_line = definition
        if not self.should_analyze(
            function_name, f"{call_text} {function_file.content}"
        ):
            return None

        if self._client is None:
            try:
                self._client = LLMClient()
            except LLMUnavailableError:
                return None

        prompt = self._prompt(
            loop_file_path=loop_file.path,
            loop_line=loop_line,
            function_name=function_name,
            other_file_path=function_file.path,
            function_line=function_line,
            loop_file_content=loop_file.content,
            other_file_content=function_file.content,
        )
        estimated_tokens = self._estimate_tokens(
            loop_file.content, function_file.content
        )

        try:
            result = generate_structured(
                self._client,
                prompt=prompt,
                response_model=_response_model(),
                category="classification",
                estimated_tokens=estimated_tokens,
            )
        except BudgetExceededError as exc:
            return Issue(
                severity=Severity.INFO,
                detector_name="llm_n_plus_one",
                description=(
                    f"Loop at {loop_file.path}:{loop_line} calls {function_name}() at "
                    f"{function_file.path}:{function_line}; unresolved — LLM budget "
                    "exceeded."
                ),
                affected_table=None,
                affected_columns=[],
                suggested_action=(
                    "No action taken automatically due to LLM budget limit."
                ),
                confidence=0.0,
                context={
                    "file": loop_file.path,
                    "line_number": loop_line,
                    "function_name": function_name,
                    "called_function_file": function_file.path,
                    "called_function_line": function_line,
                    "reason": "llm budget exceeded",
                    "error": str(exc),
                },
            )
        except (StructuredOutputError, LLMUnavailableError) as exc:
            logger.debug("LLM ambiguous-call analysis unavailable: %s", exc)
            return None

        if not result.is_n_plus_one:
            return None

        return Issue(
            severity=_confidence_to_severity(result.confidence),
            detector_name="llm_n_plus_one",
            description=result.explanation,
            affected_table=None,
            affected_columns=[],
            suggested_action=result.suggested_fix,
            confidence=result.confidence,
            context={
                "file": loop_file.path,
                "line_number": loop_line,
                "function_name": function_name,
                "called_function_file": function_file.path,
                "called_function_line": function_line,
                "call_text": call_text,
            },
        )
