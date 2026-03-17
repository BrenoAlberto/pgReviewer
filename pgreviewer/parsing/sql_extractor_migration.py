from pathlib import Path

import tree_sitter_python as tspython
import tree_sitter_sql as tssql
from tree_sitter import Language, Parser, Query, QueryCursor

from pgreviewer.core.models import ExtractedQuery

# Initialize Languages
PY_LANGUAGE = Language(tspython.language())
SQL_LANGUAGE = Language(tssql.language())

# Parser initialization
py_parser = Parser(PY_LANGUAGE)
sql_parser = Parser(SQL_LANGUAGE)


def _is_transaction_control(sql: str) -> bool:
    """Check if the statement is just BEGIN, COMMIT, or ROLLBACK."""
    normalized = sql.strip().upper().rstrip(";")
    return normalized in {
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "START TRANSACTION",
        "ABORT",
        "END",
    }


def _extract_statements_recursive(node, text_bytes, start_line, file_path, queries):
    """Helper to find all 'statement' nodes in the tree."""
    if node.type in ("statement", "query_statement", "ERROR"):
        stmt_text = text_bytes[node.start_byte : node.end_byte].decode("utf-8").strip()
        if stmt_text and stmt_text != ";" and not _is_transaction_control(stmt_text):
            if stmt_text.startswith("--") and "\n" not in stmt_text:
                return

            queries.append(
                ExtractedQuery(
                    sql=stmt_text,
                    source_file=file_path,
                    line_number=start_line + node.start_point[0],
                    extraction_method="migration_sql",
                    confidence=1.0,
                )
            )
    elif node.type in ("program", "transaction", "block", "subprogram"):
        for child in node.children:
            _extract_statements_recursive(
                child, text_bytes, start_line, file_path, queries
            )


def split_sql_statements(
    text: str, start_line: int = 1, file_path: str = ""
) -> list[ExtractedQuery]:
    """Splits SQL text into individual statements using tree-sitter-sql."""
    text_bytes = text.encode("utf-8")
    tree = sql_parser.parse(text_bytes)
    queries: list[ExtractedQuery] = []

    _extract_statements_recursive(
        tree.root_node, text_bytes, start_line, file_path, queries
    )

    return queries


def extract_from_sql_file(file_path: Path) -> list[ExtractedQuery]:
    """Extract SQL statements from a .sql file using tree-sitter."""
    content = file_path.read_text()
    return split_sql_statements(content, file_path=str(file_path))


def _synthesize_create_index_sql(call_src: str) -> str | None:
    """Parse an op.create_index(...) call and return equivalent CREATE INDEX SQL."""
    import ast

    try:
        # Wrap in a dummy expression so ast.parse can handle it
        tree_ast = ast.parse(call_src.strip(), mode="eval")
    except SyntaxError:
        return None

    if not isinstance(tree_ast.body, ast.Call):
        return None

    call = tree_ast.body
    args = call.args
    kwargs = {kw.arg: kw.value for kw in call.keywords}

    # Positional: (index_name, table_name, columns, ...)
    if len(args) < 2:
        return None

    def _str(node: ast.expr) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        return None

    def _list_of_str(node: ast.expr) -> list[str] | None:
        if isinstance(node, ast.List):
            result = []
            for elt in node.elts:
                s = _str(elt)
                if s is None:
                    return None
                result.append(s)
            return result
        return None

    index_name = _str(args[0])
    table_name = _str(args[1])
    columns = _list_of_str(args[2]) if len(args) >= 3 else None

    if not index_name or not table_name or not columns:
        return None

    unique = False
    if "unique" in kwargs:
        uv = kwargs["unique"]
        if isinstance(uv, ast.Constant):
            unique = bool(uv.value)

    concurrently = False
    if "postgresql_concurrently" in kwargs:
        cv = kwargs["postgresql_concurrently"]
        if isinstance(cv, ast.Constant):
            concurrently = bool(cv.value)

    parts = ["CREATE"]
    if unique:
        parts.append("UNIQUE")
    parts.append("INDEX")
    if concurrently:
        parts.append("CONCURRENTLY")
    parts.append(index_name)
    parts.append("ON")
    parts.append(table_name)
    parts.append(f"({', '.join(columns)})")
    return " ".join(parts)


def extract_from_alembic_file(file_path: Path) -> list[ExtractedQuery]:
    """Extract SQL from an Alembic migration file using tree-sitter-python."""
    content = file_path.read_text()
    content_bytes = content.encode("utf-8")
    tree = py_parser.parse(content_bytes)

    # ── op.execute() / op.execute(text()) → raw SQL strings ──────────────────
    execute_scm = """
    (call
      function: (attribute
        object: (identifier) @obj
        attribute: (identifier) @attr)
      arguments: (argument_list
        (string) @sql_str)
      (#eq? @obj "op")
      (#eq? @attr "execute"))

    (call
      function: (attribute
        object: (identifier) @obj
        attribute: (identifier) @attr)
      arguments: (argument_list
        (call
          function: (identifier) @func
          arguments: (argument_list (string) @sql_str)))
      (#eq? @obj "op")
      (#eq? @attr "execute")
      (#eq? @func "text"))
    """

    query = Query(PY_LANGUAGE, execute_scm)
    cursor = QueryCursor(query)
    captures = cursor.captures(tree.root_node)

    extracted: list[ExtractedQuery] = []
    sql_nodes = captures.get("sql_str", [])
    sql_nodes.sort(key=lambda n: n.start_byte)

    for node in sql_nodes:
        raw_str = content_bytes[node.start_byte : node.end_byte].decode("utf-8")

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
                sql_content = raw_str[content_start:content_end]
            else:
                sql_content = raw_str[content_start:]
        else:
            sql_content = raw_str

        inner_queries = split_sql_statements(
            sql_content, start_line=node.start_point[0] + 1, file_path=str(file_path)
        )
        extracted.extend(inner_queries)

    # ── op.create_index(...) → synthesize CREATE INDEX SQL ────────────────────
    create_index_scm = """
    (call
      function: (attribute
        object: (identifier) @obj
        attribute: (identifier) @attr)
      (#eq? @obj "op")
      (#eq? @attr "create_index")) @call_node
    """
    ci_query = Query(PY_LANGUAGE, create_index_scm)
    ci_cursor = QueryCursor(ci_query)
    ci_captures = ci_cursor.captures(tree.root_node)
    call_nodes = ci_captures.get("call_node", [])
    call_nodes.sort(key=lambda n: n.start_byte)

    for node in call_nodes:
        call_src = content_bytes[node.start_byte : node.end_byte].decode("utf-8")
        sql = _synthesize_create_index_sql(call_src)
        if sql:
            extracted.append(
                ExtractedQuery(
                    sql=sql,
                    source_file=str(file_path),
                    line_number=node.start_point[0] + 1,
                    extraction_method="alembic_op",
                    confidence=1.0,
                )
            )

    # Sort all extracted queries by line number
    extracted.sort(key=lambda q: q.line_number)
    return extracted
