"""SQLAlchemy ORM query extractor using tree-sitter.

Detects ``session.query()``, ``select()``, ``filter()``, ``where()``,
``order_by()``, and ``join()`` patterns in Python source files and produces
approximate SQL strings for analysis.

Tree-sitter queries are stored in::

    pgreviewer/parsing/treesitter/queries/python/sqlalchemy_queries.scm

The extractor finds ``session.query(Model)`` and ``select(Model)`` call nodes,
walks up the AST to find the outermost call in the chain, then walks down to
collect all chained method calls and assembles an approximate SQL string.

Public API
----------
- :func:`extract_orm_queries` – extract ORM queries from Python source text
"""

from __future__ import annotations

import logging
from pathlib import Path

import tree_sitter_python as tspython
from tree_sitter import Language, Node, Parser, Query, QueryCursor

from pgreviewer.core.models import ExtractedQuery

logger = logging.getLogger(__name__)

PY_LANGUAGE = Language(tspython.language())
_parser = Parser(PY_LANGUAGE)

_QUERIES_DIR = Path(__file__).parent / "treesitter" / "queries" / "python"
_SCM_FILE = _QUERIES_DIR / "sqlalchemy_queries.scm"

_ORM_NOTES = "ORM query — actual SQL may include additional filters at runtime"
_ORM_CONFIDENCE = 0.7
_ORM_METHOD = "treesitter_orm"

# Methods whose arguments contribute to the WHERE clause.
_FILTER_METHODS = frozenset({"filter", "where"})
# Methods using keyword-argument style (column=value).
_FILTER_BY_METHODS = frozenset({"filter_by"})


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_query() -> Query:
    """Compile and return the SQLAlchemy ORM tree-sitter query."""
    return Query(PY_LANGUAGE, _SCM_FILE.read_text())


def _model_to_table(model_name: str) -> str:
    """Convert a SQLAlchemy model class name to an approximate table name.

    Examples
    --------
    ``Order``   → ``orders``
    ``Category`` → ``categories``
    ``User``    → ``users``
    """
    name = model_name.lower()
    if name.endswith("y"):
        return name[:-1] + "ies"
    return name + "s"


def _find_chain_root(call_node: Node) -> Node:
    """Walk up the AST to find the topmost call in a method-call chain.

    Starting from *call_node* (e.g. ``session.query(Order)``), this function
    ascends through ``attribute → call`` pairs as long as the current node is
    the ``object`` of an attribute that is the ``function`` of a parent call.
    """
    node = call_node
    while True:
        parent = node.parent
        if parent is None or parent.type != "attribute":
            break
        grandparent = parent.parent
        if grandparent is None or grandparent.type != "call":
            break
        # Confirm this node is the object (not the attribute) of the parent.
        obj = parent.child_by_field_name("object")
        if obj is None or (obj.start_byte, obj.end_byte) != (
            node.start_byte,
            node.end_byte,
        ):
            break
        node = grandparent
    return node


def _collect_chain(root_call: Node) -> list[tuple[str, Node | None]]:
    """Collect ``(method_name, args_node)`` pairs from a method-call chain.

    Walks *down* from the outermost call in a chain (e.g. ``.filter()``) to
    the innermost (e.g. ``session.query()`` or ``select()``), then reverses
    the list so that entries are in call order (innermost first).
    """
    chain: list[tuple[str, Node | None]] = []
    node: Node | None = root_call

    while node is not None and node.type == "call":
        func = node.child_by_field_name("function")
        args = node.child_by_field_name("arguments")

        if func is None:
            break

        if func.type == "attribute":
            method_node = func.child_by_field_name("attribute")
            if method_node:
                chain.append((method_node.text.decode("utf-8"), args))
            # Descend into the object of the attribute (the inner call).
            node = func.child_by_field_name("object")

        elif func.type == "identifier":
            chain.append((func.text.decode("utf-8"), args))
            break

        else:
            break

    # chain is outermost-first; reverse to innermost-first (call order).
    return list(reversed(chain))


def _get_comparison_op(node: Node) -> str:
    """Return the SQL comparison operator from a ``comparison_operator`` node.

    Maps Python ``==`` to SQL ``=``; all others are kept as-is.
    """
    for child in node.children:
        if not child.is_named:
            op = child.text.decode("utf-8").strip()
            if op in ("==", "!=", "<", ">", "<=", ">="):
                return "=" if op == "==" else op
    return "="


def _extract_value(node: Node) -> str:
    """Return a SQL value representation for the given AST node.

    Identifiers become ``:name`` parameter placeholders; literals are kept
    verbatim; anything else becomes ``:param``.
    """
    t = node.type
    if t == "identifier":
        return f":{node.text.decode('utf-8')}"
    if t in ("string", "concatenated_string"):
        return node.text.decode("utf-8")
    if t == "integer":
        return node.text.decode("utf-8")
    if t == "float":
        return node.text.decode("utf-8")
    if t == "none":
        return "NULL"
    if t == "true":
        return "TRUE"
    if t == "false":
        return "FALSE"
    if t == "attribute":
        attr = node.child_by_field_name("attribute")
        if attr:
            return f":{attr.text.decode('utf-8')}"
    return ":param"


def _extract_column(node: Node) -> str | None:
    """Return the column name referenced by *node*.

    Handles ``Model.column`` attribute access and bare ``column`` identifiers.
    """
    if node.type == "attribute":
        attr = node.child_by_field_name("attribute")
        if attr:
            return attr.text.decode("utf-8")
    if node.type == "identifier":
        return node.text.decode("utf-8")
    return None


def _extract_condition(node: Node) -> str:
    """Build a SQL condition string from a single comparison or boolean node."""
    if node.type == "comparison_operator":
        named = node.named_children
        if len(named) >= 2:
            left, right = named[0], named[-1]
            column = _extract_column(left)
            value = _extract_value(right)
            op = _get_comparison_op(node)
            if column:
                return f"{column} {op} {value}"

    if node.type == "boolean_operator":
        # e.g. (cond1) and (cond2)
        parts = []
        for child in node.named_children:
            cond = _extract_condition(child)
            if cond:
                parts.append(cond)
        bool_op = "AND"
        for child in node.children:
            if not child.is_named:
                raw = child.text.decode("utf-8").strip().upper()
                if raw in ("AND", "OR"):
                    bool_op = raw
                    break
        return f" {bool_op} ".join(parts) if parts else ""

    return ""


def _build_where_from_args(args_node: Node | None) -> str:
    """Build a SQL WHERE fragment from a ``filter()``/``where()`` argument list."""
    if args_node is None:
        return ""
    conditions: list[str] = []
    for child in args_node.named_children:
        if child.type == "keyword_argument":
            # filter_by(column=value) is handled separately, but guard here too
            key = child.child_by_field_name("name")
            val = child.child_by_field_name("value")
            if key and val:
                conditions.append(f"{key.text.decode('utf-8')} = {_extract_value(val)}")
        else:
            cond = _extract_condition(child)
            if cond:
                conditions.append(cond)
    return " AND ".join(conditions)


def _build_filter_by_from_args(args_node: Node | None) -> str:
    """Build a SQL WHERE fragment from a ``filter_by()`` keyword-argument list."""
    if args_node is None:
        return ""
    conditions: list[str] = []
    for child in args_node.named_children:
        if child.type == "keyword_argument":
            key = child.child_by_field_name("name")
            val = child.child_by_field_name("value")
            if key and val:
                conditions.append(f"{key.text.decode('utf-8')} = {_extract_value(val)}")
    return " AND ".join(conditions)


def _build_orderby_from_args(args_node: Node | None) -> str:
    """Build a SQL ORDER BY fragment from an ``order_by()`` argument list."""
    if args_node is None:
        return ""
    parts: list[str] = []
    for child in args_node.named_children:
        if child.type == "attribute":
            attr = child.child_by_field_name("attribute")
            if attr:
                parts.append(attr.text.decode("utf-8"))
        elif child.type == "call":
            # e.g. desc(Model.column) or asc(Model.column)
            func = child.child_by_field_name("function")
            inner_args = child.child_by_field_name("arguments")
            if func and inner_args and inner_args.named_children:
                col = _extract_column(inner_args.named_children[0])
                direction = func.text.decode("utf-8").upper()
                if col:
                    parts.append(f"{col} {direction}")
        elif child.type == "identifier":
            parts.append(child.text.decode("utf-8"))
    return ", ".join(parts)


def _build_join_from_args(args_node: Node | None) -> str:
    """Build a SQL JOIN fragment from a ``join()`` argument list."""
    if args_node is None:
        return ""
    named = args_node.named_children
    if not named:
        return ""
    first = named[0]
    model_name: str | None = None
    if first.type == "identifier":
        model_name = first.text.decode("utf-8")
    elif first.type == "attribute":
        obj = first.child_by_field_name("object")
        if obj and obj.type == "identifier":
            model_name = obj.text.decode("utf-8")
    if model_name:
        return f"JOIN {_model_to_table(model_name)}"
    return "JOIN unknown"


def _build_sql_from_chain(chain: list[tuple[str, Node | None]]) -> str:
    """Assemble an approximate SQL SELECT string from a collected method chain.

    Parameters
    ----------
    chain:
        List of ``(method_name, args_node)`` in call order (innermost first),
        as returned by :func:`_collect_chain`.

    Returns
    -------
    str
        Approximate SQL, e.g. ``SELECT * FROM orders WHERE user_id = :user_id``.
    """
    table = "unknown"
    where_parts: list[str] = []
    order_parts: list[str] = []
    join_parts: list[str] = []

    for method_name, args_node in chain:
        if method_name in ("query", "select"):
            # Derive table name from the first model argument.
            if args_node and args_node.named_children:
                first_arg = args_node.named_children[0]
                model_name: str | None = None
                if first_arg.type == "identifier":
                    model_name = first_arg.text.decode("utf-8")
                elif first_arg.type == "attribute":
                    obj = first_arg.child_by_field_name("object")
                    if obj and obj.type == "identifier":
                        model_name = obj.text.decode("utf-8")
                if model_name:
                    table = _model_to_table(model_name)

        elif method_name in _FILTER_METHODS:
            filter_clause = _build_where_from_args(args_node)
            if filter_clause:
                where_parts.append(filter_clause)

        elif method_name in _FILTER_BY_METHODS:
            filter_by_clause = _build_filter_by_from_args(args_node)
            if filter_by_clause:
                where_parts.append(filter_by_clause)

        elif method_name == "order_by":
            order = _build_orderby_from_args(args_node)
            if order:
                order_parts.append(order)

        elif method_name == "join":
            join = _build_join_from_args(args_node)
            if join:
                join_parts.append(join)

    sql = f"SELECT * FROM {table}"
    if join_parts:
        sql += " " + " ".join(join_parts)
    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)
    if order_parts:
        sql += " ORDER BY " + ", ".join(order_parts)
    return sql


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_orm_queries(
    python_source: str,
    file_path: str = "",
) -> list[ExtractedQuery]:
    """Find SQLAlchemy ORM query patterns in *python_source* and return approximate SQL.

    Detects:

    - ``session.query(Model).filter(Model.column == value)``
    - ``select(Model).where(Model.column == value)``
    - ``.order_by(Model.column)``
    - ``.join(OtherModel, condition)``

    For each detected chain the extractor produces an :class:`ExtractedQuery`
    with ``extraction_method="treesitter_orm"`` and ``confidence=0.7``
    (lower than raw-SQL extraction because ORM queries are dynamic).

    Parameters
    ----------
    python_source:
        Raw Python source text to analyse.
    file_path:
        Optional path stored in the ``source_file`` field of each result.

    Returns
    -------
    list[ExtractedQuery]
        One entry per ORM query chain found, sorted by line number.
        Returns an empty list for blank/empty source.
    """
    if not python_source.strip():
        return []

    tree = _parser.parse(python_source.encode("utf-8"))
    query = _load_query()
    cursor = QueryCursor(query)
    caps = cursor.captures(tree.root_node)

    extracted: list[ExtractedQuery] = []
    # Track chain roots by (start_byte, end_byte) to avoid emitting duplicates
    # when a single source location has both a .query() and a .select() capture,
    # or when the same chain is captured via multiple inner calls.
    seen_roots: set[tuple[int, int]] = set()

    for capture_name in ("query_call", "select_call"):
        for call_node in caps.get(capture_name, []):
            root = _find_chain_root(call_node)
            root_key = (root.start_byte, root.end_byte)
            if root_key in seen_roots:
                continue
            seen_roots.add(root_key)

            chain = _collect_chain(root)
            sql = _build_sql_from_chain(chain)

            extracted.append(
                ExtractedQuery(
                    sql=sql,
                    source_file=file_path,
                    line_number=root.start_point[0] + 1,
                    extraction_method=_ORM_METHOD,
                    confidence=_ORM_CONFIDENCE,
                    notes=_ORM_NOTES,
                )
            )

    extracted.sort(key=lambda q: q.line_number)
    return extracted
