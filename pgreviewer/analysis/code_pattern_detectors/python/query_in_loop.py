from __future__ import annotations

import ast
import logging
import re
from fnmatch import fnmatch
from pathlib import Path
from typing import TYPE_CHECKING

from pgreviewer.analysis.call_graph import build_shallow_call_graph, resolve_to_query
from pgreviewer.analysis.code_pattern_detectors.llm_n_plus_one import (
    LLMNPlusOneAnalyzer,
)
from pgreviewer.analysis.fix_suggesters.batch_query import suggest_batch_query_fix
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
_INLINE_IGNORE_RE = re.compile(r"#\s*pgreviewer:ignore\[(?P<detectors>[^\]]+)\]")
logger = logging.getLogger(__name__)
_QUERY_ALL_RE = re.compile(
    r"\.query\(\s*(?P<model>[A-Za-z_][A-Za-z0-9_\.]*)\s*\)\.all\(\s*\)"
)


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


def _read_project_nested_list(section: str, key: str) -> list[str]:
    config_file = Path(".pgreviewer.yml")
    if not config_file.exists():
        return []

    values: list[str] = []
    in_section = False
    in_key = False
    section_indent = 0
    key_indent = 0
    for raw_line in config_file.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        if not in_section:
            if stripped == f"{section}:" and indent == 0:
                in_section = True
                section_indent = indent
            continue
        if indent <= section_indent:
            break
        if not in_key:
            if stripped == f"{key}:":
                in_key = True
                key_indent = indent
            continue
        if indent <= key_indent:
            break
        if stripped.startswith("- "):
            values.append(stripped[2:].strip().strip("'\""))
    return values


def _read_project_query_in_loop_allowlist() -> set[str]:
    return {
        name.lower()
        for name in _read_project_nested_list("function_allowlist", "query_in_loop")
        if name.strip()
    }


def _read_project_query_in_loop_ignore_patterns() -> list[str]:
    return _read_project_nested_list("ignore_patterns", "query_in_loop")


def _known_query_methods() -> set[str]:
    configured = {name.lower() for name in settings.QUERY_METHODS if name.strip()}
    configured.update(_read_project_query_methods())
    return configured


def _query_in_loop_ignore_patterns() -> list[str]:
    return [
        *settings.QUERY_IN_LOOP_IGNORE_PATTERNS,
        *_read_project_query_in_loop_ignore_patterns(),
    ]


def _query_in_loop_function_allowlist() -> set[str]:
    configured = {
        name.lower()
        for name in settings.QUERY_IN_LOOP_FUNCTION_ALLOWLIST
        if name.strip()
    }
    configured.update(_read_project_query_in_loop_allowlist())
    return configured


def _is_function_allowlisted(function_name: str, allowlist: set[str]) -> bool:
    lowered = function_name.lower()
    return lowered in allowlist or lowered.split(".")[-1] in allowlist


def _is_for_iterable(node) -> bool:
    """
    Return True if `node` is (or is a descendant of) the iterable expression
    of a for-statement — i.e. the `right` field in `for x in EXPR:`.

    Calls in that position execute exactly once before any iteration begins,
    so they are NOT per-row queries and must not be flagged as N+1.
    """
    current = node
    while current is not None:
        parent = current.parent
        if parent is None:
            break
        if parent.type == "for_statement":
            iterable = parent.child_by_field_name("right")
            if iterable is not None and iterable == current:
                return True
        current = parent
    return False


def _find_enclosing_loop(node):
    current = node.parent
    while current is not None:
        if current.type in _LOOP_NODE_TYPES:
            # Exclude calls that are the iterable expression of this loop —
            # they execute once before any iteration, not per-iteration.
            if _is_for_iterable(node):
                return None
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
    except (SyntaxError, ValueError):
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


_FETCH_ONLY_METHODS = frozenset({"fetchone", "fetchall", "fetchmany"})


def _execute_precedes_in_loop(call_node, loop_node) -> bool:
    """
    Return True when an execute() call on the same receiver as `call_node`
    appears *within the loop body* before `call_node`.

    Detects the intra-loop cursor pattern:
        for ...:
            conn.execute("SELECT ...")   # opens cursor
            row = conn.fetchone()        # ← call_node — not a new round-trip
    """
    fn = call_node.child_by_field_name("function")
    if fn is None or fn.type != "attribute":
        return False
    receiver = fn.child_by_field_name("object")
    if receiver is None:
        return False
    receiver_text = receiver.text.decode("utf-8")

    for node in _iter_nodes(loop_node):
        if node.type != "call":
            continue
        nfn = node.child_by_field_name("function")
        if nfn is None or nfn.type != "attribute":
            continue
        attr = nfn.child_by_field_name("attribute")
        obj = nfn.child_by_field_name("object")
        if attr is None or obj is None:
            continue
        if attr.text.decode("utf-8").lower() != "execute":
            continue
        if obj.text.decode("utf-8") != receiver_text:
            continue
        if node.end_byte < call_node.start_byte:
            return True
    return False


def _enclosing_function(node):
    """Return the nearest enclosing function_definition node, or None."""
    current = node.parent
    while current is not None:
        if current.type in {"function_definition", "async_function_definition"}:
            return current
        current = current.parent
    return None


def _prior_execute_on_same_receiver(call_node, loop_node, root_node) -> bool:
    """
    Return True when `call_node` (e.g. fetchone()) is consuming a cursor that
    was already opened by an execute() call on the **same receiver object**
    *before* the loop, **within the same function body**.

    Pattern detected:
        conn.execute("SELECT ...")   # outside the loop, same function
        while True:
            row = conn.fetchone()   # ← this call_node — safe, not N+1

    Scope is intentionally limited to the enclosing function to avoid matching
    execute() calls in sibling or parent functions.
    """
    fn = call_node.child_by_field_name("function")
    if fn is None or fn.type != "attribute":
        return False
    receiver = fn.child_by_field_name("object")
    if receiver is None:
        return False
    receiver_text = receiver.text.decode("utf-8")

    # Restrict search to the same function body as the loop.
    search_root = _enclosing_function(loop_node) or root_node

    _cursor_consumers = frozenset({"fetchone", "fetchall", "fetchmany"})

    for node in _iter_nodes(search_root):
        if node.type != "call":
            continue
        nfn = node.child_by_field_name("function")
        if nfn is None or nfn.type != "attribute":
            continue
        attr = nfn.child_by_field_name("attribute")
        obj = nfn.child_by_field_name("object")
        if attr is None or obj is None:
            continue
        if attr.text.decode("utf-8").lower() != "execute":
            continue
        if obj.text.decode("utf-8") != receiver_text:
            continue
        if node.end_byte >= loop_node.start_byte:
            continue
        # Skip execute() calls that were already consumed by a chained
        # fetchall()/fetchone() — their cursor is already exhausted.
        # e.g. conn.execute("SELECT ...").fetchall() — not an open cursor.
        parent = node.parent
        if (
            parent is not None
            and parent.type == "attribute"
            and parent.child_by_field_name("attribute") is not None
            and parent.child_by_field_name("attribute").text.decode("utf-8").lower()
            in _cursor_consumers
        ):
            continue
        return True
    return False


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
    return len(range(start, stop, step)) < _SMALL_LOOP_LIMIT


def _is_small_iterable(loop_node) -> bool:
    if loop_node.type != "for_statement":
        return False
    iterable = loop_node.child_by_field_name("right")
    if iterable is None:
        return False
    if iterable.type in {"list", "tuple"}:
        return len(iterable.named_children) < _SMALL_LOOP_LIMIT
    return _is_small_range_call(iterable)


def _query_assignments(root, known_methods: set[str]) -> list[tuple[str, int]]:
    assignments: list[tuple[str, int]] = []
    for node in _iter_nodes(root):
        if node.type != "assignment":
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
            assignments.append((left.text.decode("utf-8"), node.start_byte))
    assignments.sort(key=lambda item: item[1])
    return assignments


def _model_name_to_table_name(model_name: str) -> str:
    base_name = model_name.split(".")[-1]
    if not base_name:
        return model_name.lower()
    normalized = base_name.lower()
    if normalized.endswith("s"):
        return normalized
    return f"{normalized}s"


def _query_source_table_for_assignment(node) -> str | None:
    right = node.child_by_field_name("right")
    if right is None:
        return None
    expression_text = right.text.decode("utf-8")
    match = _QUERY_ALL_RE.search(expression_text)
    if match is None:
        return None
    return _model_name_to_table_name(match.group("model"))


def _iterable_query_sources(root) -> dict[str, str]:
    sources: dict[str, str] = {}
    for node in _iter_nodes(root):
        if node.type != "assignment":
            continue
        left = node.child_by_field_name("left")
        if left is None or left.type != "identifier":
            continue
        table_name = _query_source_table_for_assignment(node)
        if table_name is None:
            continue
        sources[left.text.decode("utf-8")] = table_name
    return sources


def _line_text(content: str, line_number: int) -> str | None:
    lines = content.splitlines()
    if line_number < 1 or line_number > len(lines):
        return None
    return lines[line_number - 1]


def _has_inline_detector_ignore(content: str, line_number: int, detector: str) -> bool:
    line = _line_text(content, line_number)
    if line is None:
        return False
    match = _INLINE_IGNORE_RE.search(line)
    if match is None:
        return False
    ignored = {name.strip().lower() for name in match.group("detectors").split(",")}
    return detector.lower() in ignored


class QueryInLoopDetector:
    name = "query_in_loop"

    def __init__(self, llm_analyzer: LLMNPlusOneAnalyzer | None = None) -> None:
        self._llm_analyzer = llm_analyzer or LLMNPlusOneAnalyzer()
        self.suppressed_findings: list[dict[str, object]] = []

    def _record_suppression(
        self,
        *,
        file_path: str,
        loop_line: int,
        call_line: int,
        method_name: str,
        reason: str,
    ) -> None:
        self.suppressed_findings.append(
            {
                "detector": self.name,
                "file": file_path,
                "loop_line": loop_line,
                "call_line": call_line,
                "method_name": method_name,
                "reason": reason,
            }
        )

    def detect(
        self,
        files: list[ParsedFile],
        query_catalog: QueryCatalog,
    ) -> list[Issue]:
        parser = TSParser("python")
        query_source = _QUERY_FILE.read_text(encoding="utf-8")
        query_methods = _known_query_methods()
        ignored_paths = _query_in_loop_ignore_patterns()
        function_allowlist = _query_in_loop_function_allowlist()
        call_graph = build_shallow_call_graph(files)
        issues: list[Issue] = []
        self.suppressed_findings = []

        for parsed_file in files:
            if parsed_file.language != "python":
                continue
            matches = parser.run_query(parsed_file.tree, query_source)
            query_assignments = _query_assignments(
                parsed_file.tree.root_node, query_methods
            )
            iterable_query_sources = _iterable_query_sources(parsed_file.tree.root_node)

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
                loop_line_number = loop_node.start_point[0] + 1
                call_line_number = call_node.start_point[0] + 1
                if _is_function_allowlisted(method_name, function_allowlist):
                    self._record_suppression(
                        file_path=parsed_file.path,
                        loop_line=loop_line_number,
                        call_line=call_line_number,
                        method_name=method_name,
                        reason="function_allowlist",
                    )
                    continue
                if any(fnmatch(parsed_file.path, pattern) for pattern in ignored_paths):
                    self._record_suppression(
                        file_path=parsed_file.path,
                        loop_line=loop_line_number,
                        call_line=call_line_number,
                        method_name=method_name,
                        reason="ignore_patterns",
                    )
                    continue
                if _has_inline_detector_ignore(
                    parsed_file.content,
                    loop_line_number,
                    self.name,
                ):
                    self._record_suppression(
                        file_path=parsed_file.path,
                        loop_line=loop_line_number,
                        call_line=call_line_number,
                        method_name=method_name,
                        reason="inline_comment",
                    )
                    continue

                # fetchone/fetchall inside a loop can be cursor iteration —
                # consuming rows from a single execute() opened before the
                # loop. Detect by checking for execute() on the same receiver
                # object before the loop start.
                if _prior_execute_on_same_receiver(
                    call_node, loop_node, parsed_file.tree.root_node
                ):
                    self._record_suppression(
                        file_path=parsed_file.path,
                        loop_line=loop_line_number,
                        call_line=call_line_number,
                        method_name=method_name,
                        reason="cursor_iteration",
                    )
                    continue

                loop_var, iterable = _loop_target_and_iterable(loop_node)
                from_prior_query = any(
                    name == iterable and byte_pos < loop_node.start_byte
                    for name, byte_pos in query_assignments
                )
                source_table = iterable_query_sources.get(iterable)
                if source_table is not None:
                    from_prior_query = True
                is_small_loop = _is_small_iterable(loop_node)
                if is_small_loop:
                    severity = Severity.INFO
                elif from_prior_query:
                    severity = Severity.CRITICAL
                else:
                    severity = Severity.WARNING

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
                        suggested_action=suggest_batch_query_fix(
                            {
                                "method_name": method_name,
                                "query_text": query_text,
                                "loop_variable": loop_var_text,
                                "iterable": iterable,
                            }
                        ),
                        context={
                            "file": parsed_file.path,
                            "line_number": call_line_number,
                            "method_name": method_name,
                            "loop_variable": loop_var_text,
                            "iterable": iterable,
                            "query_text": query_text,
                            "iterable_source_table": source_table,
                            "from_prior_query": from_prior_query,
                        },
                    )
                )

            seen_catalog_calls: set[tuple[int, int]] = set()
            for node in _iter_nodes(parsed_file.tree.root_node):
                call_node = node
                if node.type == "await":
                    call_node = node.named_children[0] if node.named_children else node
                if call_node.type != "call":
                    continue
                call_key = (call_node.start_byte, call_node.end_byte)
                if call_key in seen_catalog_calls:
                    continue
                seen_catalog_calls.add(call_key)

                function = call_node.child_by_field_name("function")
                if function is None:
                    continue

                method_name: str | None = None
                if function.type == "attribute":
                    method_node = function.child_by_field_name("attribute")
                    if method_node is not None:
                        method_name = method_node.text.decode("utf-8")
                elif function.type == "identifier":
                    method_name = function.text.decode("utf-8")
                if method_name is None:
                    continue

                loop_node = _find_enclosing_loop(call_node)
                if loop_node is None:
                    continue
                loop_line_number = loop_node.start_point[0] + 1
                call_line_number = call_node.start_point[0] + 1

                if method_name.lower() in query_methods:
                    # The first pass (TS query) handles expression-statement calls.
                    # Skip if already handled to avoid double-reporting.
                    if (call_node.start_byte, call_node.end_byte) in seen_calls:
                        continue
                    # fetchone/fetchall after an execute() in the same loop body
                    # is cursor iteration, not a new per-row query — suppress it.
                    if method_name.lower() in _FETCH_ONLY_METHODS and (
                        _execute_precedes_in_loop(call_node, loop_node)
                    ):
                        self._record_suppression(
                            file_path=parsed_file.path,
                            loop_line=loop_line_number,
                            call_line=call_line_number,
                            method_name=method_name,
                            reason="cursor_iteration_in_loop",
                        )
                        continue
                    # Assignment-style query in loop missed by the TS query
                    # (e.g., tasks = db.query(Task).all()) — report it directly.
                    if _is_function_allowlisted(method_name, function_allowlist):
                        self._record_suppression(
                            file_path=parsed_file.path,
                            loop_line=loop_line_number,
                            call_line=call_line_number,
                            method_name=method_name,
                            reason="function_allowlist",
                        )
                        continue
                    if any(
                        fnmatch(parsed_file.path, pattern) for pattern in ignored_paths
                    ):
                        self._record_suppression(
                            file_path=parsed_file.path,
                            loop_line=loop_line_number,
                            call_line=call_line_number,
                            method_name=method_name,
                            reason="ignore_patterns",
                        )
                        continue
                    if _has_inline_detector_ignore(
                        parsed_file.content, loop_line_number, self.name
                    ):
                        self._record_suppression(
                            file_path=parsed_file.path,
                            loop_line=loop_line_number,
                            call_line=call_line_number,
                            method_name=method_name,
                            reason="inline_comment",
                        )
                        continue
                    if _prior_execute_on_same_receiver(
                        call_node, loop_node, parsed_file.tree.root_node
                    ):
                        self._record_suppression(
                            file_path=parsed_file.path,
                            loop_line=loop_line_number,
                            call_line=call_line_number,
                            method_name=method_name,
                            reason="cursor_iteration",
                        )
                        continue
                    loop_var, iterable = _loop_target_and_iterable(loop_node)
                    from_prior_query = any(
                        name == iterable and byte_pos < loop_node.start_byte
                        for name, byte_pos in query_assignments
                    )
                    source_table = iterable_query_sources.get(iterable)
                    if source_table is not None:
                        from_prior_query = True
                    loop_var_text = loop_var if loop_var is not None else "n/a"
                    is_small_loop = _is_small_iterable(loop_node)
                    if is_small_loop:
                        severity = Severity.INFO
                    elif from_prior_query:
                        severity = Severity.CRITICAL
                    else:
                        severity = Severity.WARNING
                    query_text = _query_text_from_call(call_node)
                    query_suffix = f" Query: {query_text!r}." if query_text else ""
                    description = (
                        f"Query method '{method_name}' is called inside a loop "
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
                            suggested_action=suggest_batch_query_fix(
                                {
                                    "method_name": method_name,
                                    "query_text": query_text,
                                    "loop_variable": loop_var_text,
                                    "iterable": iterable,
                                }
                            ),
                            context={
                                "file": parsed_file.path,
                                "line_number": call_line_number,
                                "method_name": method_name,
                                "loop_variable": loop_var_text,
                                "iterable": iterable,
                                "query_text": query_text,
                                "iterable_source_table": source_table,
                                "from_prior_query": from_prior_query,
                            },
                        )
                    )
                    continue

                if _is_function_allowlisted(method_name, function_allowlist):
                    self._record_suppression(
                        file_path=parsed_file.path,
                        loop_line=loop_line_number,
                        call_line=call_line_number,
                        method_name=method_name,
                        reason="function_allowlist",
                    )
                    continue
                if any(fnmatch(parsed_file.path, pattern) for pattern in ignored_paths):
                    self._record_suppression(
                        file_path=parsed_file.path,
                        loop_line=loop_line_number,
                        call_line=call_line_number,
                        method_name=method_name,
                        reason="ignore_patterns",
                    )
                    continue
                if _has_inline_detector_ignore(
                    parsed_file.content,
                    loop_line_number,
                    self.name,
                ):
                    self._record_suppression(
                        file_path=parsed_file.path,
                        loop_line=loop_line_number,
                        call_line=call_line_number,
                        method_name=method_name,
                        reason="inline_comment",
                    )
                    continue

                matched_functions = query_catalog.find_by_function_name(method_name)
                if matched_functions:
                    primary_fqn = sorted(matched_functions.keys())[0]
                    primary_match = matched_functions[primary_fqn]
                    catalog_matches = sorted(matched_functions.keys())
                else:
                    primary_match = resolve_to_query(
                        method_name,
                        call_graph,
                        query_catalog,
                        max_depth=2,
                    )
                    if primary_match is None:
                        logger.debug(
                            "unresolved call in loop: function=%s file=%s line=%s",
                            method_name,
                            parsed_file.path,
                            call_node.start_point[0] + 1,
                        )
                        llm_issue = self._llm_analyzer.analyze_uncertain_call(
                            files=files,
                            loop_file=parsed_file,
                            loop_line=call_node.start_point[0] + 1,
                            function_name=method_name,
                            call_text=function.text.decode("utf-8"),
                        )
                        if llm_issue is not None:
                            issues.append(llm_issue)
                        continue
                    catalog_matches = sorted(
                        fqn
                        for fqn, info in query_catalog.functions.items()
                        if info == primary_match
                    )
                    primary_fqn = (
                        catalog_matches[0]
                        if catalog_matches
                        else f"<resolved:{method_name}>"
                    )

                if not catalog_matches:
                    catalog_matches = [primary_fqn]

                loop_var, iterable = _loop_target_and_iterable(loop_node)
                loop_var_text = loop_var if loop_var is not None else "n/a"
                source_table = iterable_query_sources.get(iterable)
                from_prior_query = (
                    source_table is not None
                    or any(
                        name == iterable and byte_pos < loop_node.start_byte
                        for name, byte_pos in query_assignments
                    )
                )
                is_small_loop = _is_small_iterable(loop_node)
                if is_small_loop:
                    _severity = Severity.INFO
                elif from_prior_query:
                    _severity = Severity.CRITICAL
                else:
                    _severity = Severity.WARNING
                call_display_name = function.text.decode("utf-8")
                description = (
                    f"Loop at {parsed_file.path}:{loop_line_number} calls "
                    f"{call_display_name}() which executes a query at "
                    f"{primary_match.file}:{primary_match.line}. "
                    f"(variable: {loop_var_text}, iterable: {iterable})."
                )
                issues.append(
                    Issue(
                        severity=_severity,
                        detector_name=self.name,
                        description=description,
                        affected_table=None,
                        affected_columns=[],
                        suggested_action=suggest_batch_query_fix(
                            {
                                "method_name": method_name,
                                "query_text": primary_match.query_text_if_available,
                                "loop_variable": loop_var_text,
                                "iterable": iterable,
                            }
                        ),
                        context={
                            "file": parsed_file.path,
                            "line_number": call_line_number,
                            "method_name": method_name,
                            "loop_variable": loop_var_text,
                            "iterable": iterable,
                            "iterable_source_table": source_table,
                            "catalog_matches": catalog_matches,
                            "call_chain": {
                                "loop": {
                                    "file": parsed_file.path,
                                    "line_number": loop_line_number,
                                    "code": _line_text(
                                        parsed_file.content, loop_line_number
                                    ),
                                },
                                "call": {
                                    "file": parsed_file.path,
                                    "line_number": call_line_number,
                                    "code": _line_text(
                                        parsed_file.content, call_line_number
                                    ),
                                    "function": call_display_name,
                                },
                                "query": {
                                    "file": primary_match.file,
                                    "line_number": primary_match.line,
                                    "catalog_function": primary_fqn,
                                    "method_name": primary_match.method_name,
                                    "query_text": primary_match.query_text_if_available,
                                },
                            },
                        },
                    )
                )

        return issues
