from __future__ import annotations

import ast
from pathlib import Path
from typing import TYPE_CHECKING

from pgreviewer.config import settings
from pgreviewer.core.models import Issue, Severity
from pgreviewer.parsing.treesitter import LANGUAGES, TSParser

if TYPE_CHECKING:
    from pgreviewer.analysis.code_pattern_detectors.base import (
        ParsedFile,
        QueryCatalog,
    )

_LOOP_NODE_TYPES = frozenset({"for_statement", "while_statement"})
_QUERY_FILE = LANGUAGES[".py"].query_dir / "loops_with_query_calls.scm"
_SMALL_LOOP_LIMIT = 10


def _iter_nodes(root):
    stack = [root]
    while stack:
        node = stack.pop()
        yield node
        stack.extend(reversed(node.children))


def _read_project_query_methods() -> set[str]:
    config_file = Path(".pgreviewer.yml")
    if not config_file.exists():
        return set()

    methods: set[str] = set()
    in_query_methods = False
    for raw_line in config_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if not in_query_methods:
            if line == "query_methods:":
                in_query_methods = True
            continue
        if line.startswith("- "):
            methods.add(line[2:].strip().strip("'\"").lower())
            continue
        break
    return methods


def _known_query_methods() -> set[str]:
    configured = {name.lower() for name in settings.QUERY_METHODS if name.strip()}
    configured.update(_read_project_query_methods())
    return configured


def _find_enclosing_loop(node):
    current = node.parent
    while current is not None:
        if current.type in _LOOP_NODE_TYPES:
            return current
        current = current.parent
    return None


def _string_literal_text(node) -> str | None:
    if node.type not in {"string", "concatenated_string"}:
        return None
    if any(child.type == "interpolation" for child in node.children):
        return None
    try:
        value = ast.literal_eval(node.text.decode("utf-8"))
    except Exception:
        return None
    return value if isinstance(value, str) else None


def _query_text_from_call(call_node) -> str | None:
    args_node = call_node.child_by_field_name("arguments")
    if args_node is None or not args_node.named_children:
        return None
    first_arg = args_node.named_children[0]
    if first_arg.type == "keyword_argument":
        first_arg = first_arg.child_by_field_name("value")
        if first_arg is None:
            return None
    return _string_literal_text(first_arg)


def _loop_target_and_iterable(loop_node) -> tuple[str | None, str]:
    if loop_node.type == "for_statement":
        target = loop_node.child_by_field_name("left")
        iterable = loop_node.child_by_field_name("right")
        return (
            target.text.decode("utf-8") if target is not None else None,
            iterable.text.decode("utf-8") if iterable is not None else "unknown",
        )

    condition = loop_node.child_by_field_name("condition")
    return None, condition.text.decode("utf-8") if condition is not None else "unknown"


def _is_small_range_call(node) -> bool:
    if node.type != "call":
        return False
    function = node.child_by_field_name("function")
    if function is None or function.type != "identifier" or function.text != b"range":
        return False
    args = node.child_by_field_name("arguments")
    if args is None or not args.named_children:
        return False
    if len(args.named_children) > 3:
        return False
    values: list[int] = []
    for arg in args.named_children:
        if arg.type != "integer":
            return False
        values.append(int(arg.text.decode("utf-8")))
    if len(values) == 1:
        start, stop, step = 0, values[0], 1
    elif len(values) == 2:
        start, stop = values
        step = 1
    else:
        start, stop, step = values
    if step == 0:
        return False
    span = stop - start
    if span <= 0:
        return True
    return (span // abs(step)) <= _SMALL_LOOP_LIMIT


def _is_small_iterable(loop_node) -> bool:
    if loop_node.type != "for_statement":
        return False
    iterable = loop_node.child_by_field_name("right")
    if iterable is None:
        return False
    if iterable.type in {"list", "tuple", "set"}:
        return len(iterable.named_children) <= _SMALL_LOOP_LIMIT
    return _is_small_range_call(iterable)


def _query_assigned_names_before(
    root, known_methods: set[str], max_byte: int
) -> set[str]:
    names: set[str] = set()
    for node in _iter_nodes(root):
        if node.type != "assignment" or node.start_byte >= max_byte:
            continue
        left = node.child_by_field_name("left")
        right = node.child_by_field_name("right")
        if left is None or left.type != "identifier" or right is None:
            continue
        call_node = right
        if right.type == "await":
            call_node = right.named_children[0] if right.named_children else right
        if call_node.type != "call":
            continue
        function = call_node.child_by_field_name("function")
        if function is None or function.type != "attribute":
            continue
        method_node = function.child_by_field_name("attribute")
        if method_node is None:
            continue
        method_name = method_node.text.decode("utf-8").lower()
        if method_name in known_methods:
            names.add(left.text.decode("utf-8"))
    return names


class QueryInLoopDetector:
    name = "query_in_loop"

    def detect(
        self, files: list[ParsedFile], query_catalog: QueryCatalog  # noqa: ARG002
    ) -> list[Issue]:
        parser = TSParser("python")
        query_source = _QUERY_FILE.read_text(encoding="utf-8")
        query_methods = _known_query_methods()
        issues: list[Issue] = []

        for parsed_file in files:
            if parsed_file.language != "python":
                continue
            parser.parse_file(parsed_file.content, language="python")
            matches = parser.run_query(parsed_file.tree, query_source)

            query_calls = [
                match["node"] for match in matches if match["capture"] == "query_call"
            ]
            seen_calls: set[tuple[int, int]] = set()

            for call_node in query_calls:
                call_key = (call_node.start_byte, call_node.end_byte)
                if call_key in seen_calls:
                    continue
                seen_calls.add(call_key)

                function = call_node.child_by_field_name("function")
                if function is None or function.type != "attribute":
                    continue
                method_node = function.child_by_field_name("attribute")
                if method_node is None:
                    continue
                method_name = method_node.text.decode("utf-8").lower()
                if method_name not in query_methods:
                    continue

                loop_node = _find_enclosing_loop(call_node)
                if loop_node is None:
                    continue

                loop_var, iterable = _loop_target_and_iterable(loop_node)
                prior_query_assignments = _query_assigned_names_before(
                    parsed_file.tree.root_node, query_methods, loop_node.start_byte
                )
                from_prior_query = iterable in prior_query_assignments
                severity = (
                    Severity.WARNING
                    if _is_small_iterable(loop_node) and not from_prior_query
                    else Severity.CRITICAL
                )

                query_text = _query_text_from_call(call_node)
                query_suffix = f" Query: {query_text!r}." if query_text else ""
                loop_var_text = loop_var if loop_var is not None else "n/a"
                description = (
                    f"Query method '{method_name}' is called directly inside a loop "
                    f"(variable: {loop_var_text}, iterable: {iterable})."
                    f"{query_suffix}"
                )
                if from_prior_query:
                    description += (
                        " Iterable appears to come from a previous query result, "
                        "which strongly hints at an N+1 access pattern."
                    )

                issues.append(
                    Issue(
                        severity=severity,
                        detector_name=self.name,
                        description=description,
                        affected_table=None,
                        affected_columns=[],
                        suggested_action=(
                            "Batch related IDs and fetch data in a single query "
                            "outside the loop."
                        ),
                        context={
                            "file": parsed_file.path,
                            "line_number": call_node.start_point[0] + 1,
                            "method_name": method_name,
                            "loop_variable": loop_var,
                            "iterable": iterable,
                            "query_text": query_text,
                        },
                    )
                )

        return issues
