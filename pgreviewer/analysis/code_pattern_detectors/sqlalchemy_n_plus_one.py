from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pgreviewer.core.models import Issue, Severity
from pgreviewer.parsing.sqlalchemy_analyzer import analyze_model_source
from pgreviewer.parsing.treesitter import LANGUAGES, TSParser

if TYPE_CHECKING:
    from pgreviewer.analysis.code_pattern_detectors.base import ParsedFile, QueryCatalog

_ATTRIBUTE_ACCESS_QUERY_FILE = (
    LANGUAGES[".py"].query_dir / "sqlalchemy_n_plus_one.scm"
)
_EAGER_LOADERS = frozenset({"joinedload", "selectinload", "subqueryload"})


def _iter_nodes(root):
    stack = [root]
    while stack:
        node = stack.pop()
        yield node
        stack.extend(reversed(node.children))


@dataclass(frozen=True)
class _QueryAssignment:
    variable_name: str
    model_name: str
    eager_relationships: set[str]
    start_byte: int


def _call_name(call_node) -> str | None:
    function = call_node.child_by_field_name("function")
    if function is None:
        return None
    if function.type == "identifier":
        return function.text.decode("utf-8")
    if function.type == "attribute":
        attribute = function.child_by_field_name("attribute")
        if attribute is not None:
            return attribute.text.decode("utf-8")
    return None


def _first_positional_arg(call_node):
    args = call_node.child_by_field_name("arguments")
    if args is None:
        return None
    for arg in args.named_children:
        if arg.type != "keyword_argument":
            return arg
    return None


def _extract_model_name(assignment_right) -> str | None:
    for node in _iter_nodes(assignment_right):
        if node.type != "call":
            continue
        if _call_name(node) != "query":
            continue
        first_arg = _first_positional_arg(node)
        if first_arg is None or first_arg.type != "identifier":
            return None
        return first_arg.text.decode("utf-8")
    return None


def _extract_eager_relationships(assignment_right, model_name: str) -> set[str]:
    eager_relationships: set[str] = set()
    for node in _iter_nodes(assignment_right):
        if node.type != "call":
            continue
        if _call_name(node) not in _EAGER_LOADERS:
            continue
        first_arg = _first_positional_arg(node)
        if first_arg is None or first_arg.type != "attribute":
            continue
        obj_node = first_arg.child_by_field_name("object")
        attr_node = first_arg.child_by_field_name("attribute")
        if obj_node is None or attr_node is None:
            continue
        if obj_node.type != "identifier" or obj_node.text.decode("utf-8") != model_name:
            continue
        eager_relationships.add(attr_node.text.decode("utf-8"))
    return eager_relationships


def _collect_query_assignments(
    parsed_file: ParsedFile,
) -> dict[str, list[_QueryAssignment]]:
    assignments: dict[str, list[_QueryAssignment]] = {}
    for node in _iter_nodes(parsed_file.tree.root_node):
        if node.type != "assignment":
            continue
        left = node.child_by_field_name("left")
        right = node.child_by_field_name("right")
        if left is None or right is None or left.type != "identifier":
            continue
        model_name = _extract_model_name(right)
        if model_name is None:
            continue
        variable_name = left.text.decode("utf-8")
        assignments.setdefault(variable_name, []).append(
            _QueryAssignment(
                variable_name=variable_name,
                model_name=model_name,
                eager_relationships=_extract_eager_relationships(right, model_name),
                start_byte=node.start_byte,
            )
        )
    for variable_assignments in assignments.values():
        variable_assignments.sort(key=lambda assignment: assignment.start_byte)
    return assignments


def _collect_relationships_by_model(files: list[ParsedFile]) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for parsed_file in files:
        if parsed_file.language != "python":
            continue
        for model in analyze_model_source(parsed_file.content, parsed_file.path):
            result.setdefault(model.class_name, set()).update(
                rel.name for rel in model.relationships
            )
    return result


class SQLAlchemyNPlusOneDetector:
    name = "sqlalchemy_n_plus_one"

    def detect(
        self, files: list[ParsedFile], query_catalog: QueryCatalog
    ) -> list[Issue]:
        del query_catalog
        parser = TSParser("python")
        attribute_query = _ATTRIBUTE_ACCESS_QUERY_FILE.read_text(encoding="utf-8")
        model_relationships = _collect_relationships_by_model(files)
        issues: list[Issue] = []

        for parsed_file in files:
            if parsed_file.language != "python":
                continue
            query_assignments = _collect_query_assignments(parsed_file)
            if not query_assignments:
                continue

            for node in _iter_nodes(parsed_file.tree.root_node):
                if node.type != "for_statement":
                    continue
                loop_target = node.child_by_field_name("left")
                iterable = node.child_by_field_name("right")
                body = node.child_by_field_name("body")
                if (
                    loop_target is None
                    or iterable is None
                    or body is None
                    or loop_target.type != "identifier"
                    or iterable.type != "identifier"
                ):
                    continue

                iterable_name = iterable.text.decode("utf-8")
                assignments_for_variable = query_assignments.get(iterable_name, [])
                assignment = next(
                    (
                        candidate
                        for candidate in reversed(assignments_for_variable)
                        if candidate.start_byte < node.start_byte
                    ),
                    None,
                )
                if assignment is None:
                    continue

                loop_variable = loop_target.text.decode("utf-8")
                relationship_names = model_relationships.get(
                    assignment.model_name, set()
                )
                if not relationship_names:
                    continue

                seen_relations: set[str] = set()
                for match in parser.run_query(parsed_file.tree, attribute_query):
                    if match["capture"] != "attribute_access":
                        continue
                    attr_node = match["node"]
                    if not (
                        body.start_byte <= attr_node.start_byte < body.end_byte
                        and attr_node.end_byte <= body.end_byte
                    ):
                        continue

                    object_node = attr_node.child_by_field_name("object")
                    attribute_node = attr_node.child_by_field_name("attribute")
                    if (
                        object_node is None
                        or attribute_node is None
                        or object_node.type != "identifier"
                        or attribute_node.type != "identifier"
                    ):
                        continue
                    if object_node.text.decode("utf-8") != loop_variable:
                        continue
                    relationship_name = attribute_node.text.decode("utf-8")
                    if relationship_name not in relationship_names:
                        continue
                    if relationship_name in assignment.eager_relationships:
                        continue
                    if relationship_name in seen_relations:
                        continue
                    seen_relations.add(relationship_name)
                    selectinload_hint = (
                        f"selectinload({assignment.model_name}.{relationship_name})"
                    )
                    joinedload_hint = (
                        f"joinedload({assignment.model_name}.{relationship_name})"
                    )

                    issues.append(
                        Issue(
                            severity=Severity.CRITICAL,
                            detector_name=self.name,
                            description=(
                                f"Potential SQLAlchemy N+1 detected: loop variable "
                                f"'{loop_variable}' accesses relationship "
                                f"'{relationship_name}' loaded from query result "
                                f"'{iterable_name}'."
                            ),
                            affected_table=None,
                            affected_columns=[],
                            suggested_action=(
                                f"Add `{selectinload_hint}` "
                                f"or `{joinedload_hint}` to the query."
                            ),
                            context={
                                "file": parsed_file.path,
                                "line_number": attr_node.start_point[0] + 1,
                                "loop_variable": loop_variable,
                                "iterable": iterable_name,
                                "model_name": assignment.model_name,
                                "relationship": relationship_name,
                            },
                        )
                    )

        return issues
