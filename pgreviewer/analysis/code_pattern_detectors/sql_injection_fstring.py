"""Detector for SQL injection via f-string interpolation in execute() calls."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import tree_sitter_python as tspython
from tree_sitter import Language, Parser

from pgreviewer.core.models import Issue, Severity

if TYPE_CHECKING:
    from pgreviewer.analysis.code_pattern_detectors.base import (
        ParsedFile,
        QueryCatalog,
    )

logger = logging.getLogger(__name__)

_PY_LANGUAGE = Language(tspython.language())
_parser = Parser(_PY_LANGUAGE)

_EXECUTE_METHODS = frozenset(
    {"execute", "fetch", "fetchrow", "fetchval", "fetchone", "fetchall"}
)


def _iter_nodes(root):
    stack = [root]
    while stack:
        node = stack.pop()
        yield node
        stack.extend(reversed(node.children))


def _is_dynamic_sql(node) -> bool:
    """Return True if node is an f-string or string built with concatenation/join."""
    if node.type == "string":
        return any(c.type == "interpolation" for c in node.children)
    if node.type == "concatenated_string":
        return any(_is_dynamic_sql(c) for c in node.children if c.is_named)
    if node.type == "binary_operator":
        left = node.child_by_field_name("left")
        right = node.child_by_field_name("right")
        op = node.child_by_field_name("operator")
        if op and op.type == "+":
            return any(_is_dynamic_sql(n) for n in (left, right) if n is not None)
    return False


def _resolve_assignment(
    root, var_name: str, before_byte: int
) -> tuple[object | None, int | None]:
    """Return (RHS node, assignment start line) for the most recent assignment
    to var_name before before_byte, or (None, None) if not found."""
    best = None
    best_end = -1
    best_line: int | None = None
    for node in _iter_nodes(root):
        if node.type != "assignment":
            continue
        if node.end_byte >= before_byte:
            continue
        left = node.child_by_field_name("left")
        right = node.child_by_field_name("right")
        if left is None or right is None:
            continue
        if (
            left.type == "identifier"
            and left.text.decode() == var_name
            and node.end_byte > best_end
        ):
            best = right
            best_end = node.end_byte
            best_line = node.start_point[0] + 1
    return best, best_line


def _unwrap_first_sql_arg(node):
    """
    Given a call node's first argument, unwrap:
      - keyword argument wrappers
      - SQLAlchemy text("...") wrappers
    Returns the innermost node that should contain the SQL string.
    """
    if node.type == "keyword_argument":
        node = node.child_by_field_name("value")
        if node is None:
            return None

    if node.type == "call":
        fn = node.child_by_field_name("function")
        if fn is not None and fn.text == b"text":
            inner_args = node.child_by_field_name("arguments")
            if inner_args and inner_args.named_children:
                inner = inner_args.named_children[0]
                if inner.type == "keyword_argument":
                    inner = inner.child_by_field_name("value")
                return inner
    return node


class FStringInjectDetector:
    """Flag execute() calls whose SQL is built with f-string or string concatenation.

    String interpolation in SQL is the most common source of SQL injection.
    Even when the interpolated values appear to come from trusted sources, the
    pattern should be replaced with parameterised queries — both for safety and
    because pgReviewer cannot analyse dynamic SQL accurately.
    """

    name = "sql_injection_fstring"

    def detect(
        self,
        files: list[ParsedFile],
        query_catalog: QueryCatalog,
    ) -> list[Issue]:
        issues: list[Issue] = []
        for parsed_file in files:
            if parsed_file.language != "python":
                continue
            issues.extend(self._check_file(parsed_file))
        return issues

    def _check_file(self, parsed_file: ParsedFile) -> list[Issue]:
        issues: list[Issue] = []
        root = parsed_file.tree.root_node
        seen: set[tuple[int, int]] = set()

        for node in _iter_nodes(root):
            if node.type != "call":
                continue
            key = (node.start_byte, node.end_byte)
            if key in seen:
                continue

            fn = node.child_by_field_name("function")
            if fn is None or fn.type != "attribute":
                continue
            method_node = fn.child_by_field_name("attribute")
            if method_node is None:
                continue
            method_name = method_node.text.decode()
            if method_name.lower() not in _EXECUTE_METHODS:
                continue

            args = node.child_by_field_name("arguments")
            if args is None or not args.named_children:
                continue

            first_arg = _unwrap_first_sql_arg(args.named_children[0])
            if first_arg is None:
                continue

            dynamic = False
            start_line: int | None = None
            if _is_dynamic_sql(first_arg):
                dynamic = True
                start_line = node.start_point[0] + 1
            elif first_arg.type == "identifier":
                resolved, assignment_line = _resolve_assignment(
                    root, first_arg.text.decode(), node.start_byte
                )
                if resolved is not None and _is_dynamic_sql(resolved):
                    dynamic = True
                    start_line = assignment_line

            if not dynamic:
                continue

            seen.add(key)
            call_line = node.start_point[0] + 1
            issues.append(
                Issue(
                    detector_name=self.name,
                    severity=Severity.CRITICAL,
                    description=(
                        f"SQL passed to `{method_name}()` is built with string "
                        "interpolation — SQL injection risk"
                    ),
                    affected_table=None,
                    affected_columns=[],
                    suggested_action=(
                        "Replace string interpolation with bound parameters:\n"
                        "```python\n"
                        "# Bad\n"
                        "sql = f\"SELECT ... WHERE col = '{val}'\"\n"
                        "db.execute(text(sql))\n\n"
                        "# Good\n"
                        "db.execute("
                        'text("SELECT ... WHERE col = :val"), {"val": val})\n'
                        "```"
                    ),
                    context={
                        "file": parsed_file.path,
                        "line_number": call_line,
                        "start_line": start_line or call_line,
                    },
                )
            )

        return issues
