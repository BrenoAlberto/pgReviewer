import logging

import tree_sitter_python as tspython
from tree_sitter import Language, Parser, Query, QueryCursor

from pgreviewer.core.models import ExtractedQuery

logger = logging.getLogger(__name__)

PY_LANGUAGE = Language(tspython.language())
parser = Parser(PY_LANGUAGE)


def _unquote(raw_bytes: bytes) -> str:
    raw_str = raw_bytes.decode("utf-8")
    first_quote_idx = -1
    quote_type = None
    for q in ['"""', "'''", '"', "'"]:
        idx = raw_str.find(q)
        if idx != -1 and (first_quote_idx == -1 or idx < first_quote_idx):
            first_quote_idx = idx
            quote_type = q

    if first_quote_idx != -1:
        content_start = first_quote_idx + len(quote_type)
        content_end = raw_str.rfind(quote_type)
        if content_end != -1 and content_end > content_start:
            return raw_str[content_start:content_end]
        else:
            return raw_str[content_start:]
    return raw_str


def extract_raw_sql(python_source: str, file_path: str = "") -> list[ExtractedQuery]:
    """Finds raw SQL strings passed to dynamic query execution methods.

    Supports:
        - cursor.execute("...")
        - ds.fetch("...")
        - text("...") helpers
        - simple string assignments: sql = "..." -> .execute(sql)
    """
    if not python_source.strip():
        return []

    tree = parser.parse(python_source.encode("utf-8"))

    # 1. Collect assignments: var_name = "string"
    assign_q = Query(
        PY_LANGUAGE,
        """
    (assignment
      left: (identifier) @var_name
      right: (string) @val)
    """,
    )
    c2 = QueryCursor(assign_q)
    captures = c2.captures(tree.root_node)

    var_nodes = captures.get("var_name", [])
    val_nodes = captures.get("val", [])

    assignments = []
    for v1, v2 in zip(var_nodes, val_nodes, strict=False):
        has_interp = any(c.type == "interpolation" for c in v2.children)
        if not has_interp:
            assignments.append(
                (v1.text.decode("utf-8"), _unquote(v2.text), v1.start_point[0])
            )

    # 2. Extract method calls
    query_scm = """
    (call
      function: (attribute
        attribute: (identifier) @method_name)
      arguments: (argument_list) @args
      (#match? @method_name "^(execute|fetch|fetchrow|fetchval|fetchone|fetchall)$"))
    """

    query = Query(PY_LANGUAGE, query_scm)
    cursor = QueryCursor(query)

    call_captures = cursor.captures(tree.root_node)
    args_nodes = call_captures.get("args", [])

    extracted: list[ExtractedQuery] = []

    for a in args_nodes:
        if not a.named_children:
            continue

        first_arg = a.named_children[0]
        call_line = a.start_point[0]

        # Unpack keyword arguments, e.g., execute(sql="...")
        if first_arg.type == "keyword_argument":
            first_arg = first_arg.child_by_field_name("value")
            if not first_arg:
                continue

        # Unpack SQLAlchemy text("...") wrapper
        if first_arg.type == "call":
            func_node = first_arg.child_by_field_name("function")
            if func_node and func_node.text == b"text":
                text_args = first_arg.child_by_field_name("arguments")
                if text_args and text_args.named_children:
                    first_arg = text_args.named_children[0]
                    # if it's a keyword argument inside text() -> unwrap it too
                    if first_arg.type == "keyword_argument":
                        first_arg = first_arg.child_by_field_name("value")
                        if not first_arg:
                            continue

        sql = first_arg.text.decode("utf-8")
        conf = 0.3

        if first_arg.type == "string":
            has_interp = any(c.type == "interpolation" for c in first_arg.children)
            if not has_interp:
                sql = _unquote(first_arg.text)
                conf = 0.9
        elif first_arg.type == "identifier":
            var_name = first_arg.text.decode("utf-8")
            closest = None
            for av_name, av_val, av_line in assignments:
                if (
                    av_name == var_name
                    and av_line <= call_line
                    and (closest is None or av_line > closest[2])
                ):
                    closest = (av_name, av_val, av_line)
            if closest:
                sql = closest[1]
                conf = 0.9

        if conf < 0.9:
            logger.info(
                "Complex SQL structure detected at %s:%s. "
                "Confidence set to 0.3. Mark for LLM fallback.",
                file_path or "unknown",
                call_line + 1,
            )

        extracted.append(
            ExtractedQuery(
                sql=sql,
                source_file=file_path,
                line_number=call_line + 1,
                extraction_method="treesitter",
                confidence=conf,
            )
        )

    extracted.sort(key=lambda q: q.line_number)
    return extracted
