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


def extract_from_alembic_file(file_path: Path) -> list[ExtractedQuery]:
    """Extract SQL from an Alembic migration file using tree-sitter-python."""
    content = file_path.read_text()
    content_bytes = content.encode("utf-8")
    tree = py_parser.parse(content_bytes)

    query_scm = """
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

    query = Query(PY_LANGUAGE, query_scm)
    cursor = QueryCursor(query)
    captures = cursor.captures(tree.root_node)

    extracted: list[ExtractedQuery] = []
    sql_nodes = captures.get("sql_str", [])

    # Sort nodes by their position in the file to preserve order
    sql_nodes.sort(key=lambda n: n.start_byte)

    for node in sql_nodes:
        raw_str = content_bytes[node.start_byte : node.end_byte].decode("utf-8")

        # Unquoting logic
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

    return extracted
