from __future__ import annotations

from typing import TYPE_CHECKING

from pgreviewer.parsing.treesitter import TSParser

if TYPE_CHECKING:
    from pgreviewer.analysis.code_pattern_detectors.base import ParsedFile
    from pgreviewer.analysis.query_catalog import QueryCatalog, QueryFunctionInfo

type CallGraph = dict[str, set[str]]

_FUNCTION_DEFINITIONS_QUERY = """
(function_definition
  name: (identifier) @function_name
) @function_definition
"""

_CALLS_QUERY = """
(call) @call
"""


def _definition_name(node) -> str | None:
    name_node = node.child_by_field_name("name")
    if name_node is None:
        return None
    return name_node.text.decode("utf-8")


def _enclosing_function_node(node):
    current = node
    while current is not None:
        if current.type == "function_definition":
            return current
        current = current.parent
    return None


def _call_name(call_node) -> str | None:
    function = call_node.child_by_field_name("function")
    if function is None:
        return None
    if function.type == "identifier":
        return function.text.decode("utf-8")
    if function.type == "attribute":
        attr = function.child_by_field_name("attribute")
        return attr.text.decode("utf-8") if attr is not None else None
    return None


def build_shallow_call_graph(files: list[ParsedFile]) -> CallGraph:
    parser = TSParser("python")
    call_graph: CallGraph = {}

    for parsed_file in files:
        if parsed_file.language != "python":
            continue

        function_matches = parser.run_query(
            parsed_file.tree, _FUNCTION_DEFINITIONS_QUERY
        )
        for match in function_matches:
            if match["capture"] != "function_definition":
                continue
            function_name = _definition_name(match["node"])
            if function_name is not None:
                call_graph.setdefault(function_name, set())

        call_matches = parser.run_query(parsed_file.tree, _CALLS_QUERY)
        seen_calls: set[tuple[int, int]] = set()
        for match in call_matches:
            if match["capture"] != "call":
                continue
            call_node = match["node"]
            call_key = (call_node.start_byte, call_node.end_byte)
            if call_key in seen_calls:
                continue
            seen_calls.add(call_key)

            enclosing_function = _enclosing_function_node(call_node)
            if enclosing_function is None:
                continue
            enclosing_name = _definition_name(enclosing_function)
            if enclosing_name is None:
                continue
            called_name = _call_name(call_node)
            if called_name is None:
                continue
            call_graph.setdefault(enclosing_name, set()).add(called_name)

    return call_graph


def _first_catalog_match(
    function_name: str, catalog: QueryCatalog
) -> QueryFunctionInfo | None:
    matches = catalog.find_by_function_name(function_name)
    if not matches:
        return None
    first_fqn = sorted(matches.keys())[0]
    return matches[first_fqn]


def resolve_to_query(
    function_name: str,
    call_graph: CallGraph,
    catalog: QueryCatalog,
    max_depth: int = 2,
) -> QueryFunctionInfo | None:
    to_visit: list[tuple[str, int]] = [(function_name, 0)]
    visited: set[str] = set()

    while to_visit:
        current_name, depth = to_visit.pop(0)
        if current_name in visited:
            continue
        visited.add(current_name)

        match = _first_catalog_match(current_name, catalog)
        if match is not None:
            return match

        if depth >= max_depth:
            continue

        for called_name in sorted(call_graph.get(current_name, set())):
            if called_name not in visited:
                to_visit.append((called_name, depth + 1))

    return None
