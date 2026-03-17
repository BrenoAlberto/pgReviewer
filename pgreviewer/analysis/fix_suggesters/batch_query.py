from __future__ import annotations

import re
from typing import Any

_SIMPLE_BATCHABLE_RE = re.compile(
    r"^\s*select\s+.+?\s+from\s+(?P<table>[a-zA-Z_][\w.]*)\s+where\s+"
    r"(?P<column>[a-zA-Z_][\w.]*)\s*=\s*(?:%s|\$\d+|:[a-zA-Z_]\w*|\?)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_ASYNCPG_CAST_RE = re.compile(r"\$\d+::([a-zA-Z_][\w]*)", re.IGNORECASE)
_ASYNCPG_METHODS = frozenset({"fetch", "fetchrow", "fetchval"})
# Default type token stays aligned with ANY($1::int[]); [] is appended in formatting.
_DEFAULT_ASYNCPG_ARRAY_TYPE = "int"


def _extract_table_and_column(query_text: str | None) -> tuple[str | None, str | None]:
    if not query_text:
        return None, None
    query_match = _SIMPLE_BATCHABLE_RE.match(query_text)
    if query_match is None:
        return None, None
    return query_match.group("table"), query_match.group("column")


def _extract_asyncpg_array_type(query_text: str | None) -> str:
    if not query_text:
        return _DEFAULT_ASYNCPG_ARRAY_TYPE
    cast_match = _ASYNCPG_CAST_RE.search(query_text)
    if cast_match is None:
        return _DEFAULT_ASYNCPG_ARRAY_TYPE
    return cast_match.group(1)


def suggest_batch_query_fix(context: dict[str, Any]) -> str:
    """Build a copyable N+1 batch-query fix snippet.

    Expected context keys:
    - method_name (str): query method (e.g. execute/fetch/fetchrow/fetchval)
    - query_text (str | None): original SQL query text when available
    - loop_variable (str): loop variable used inside the N+1 pattern
    - iterable (str): iterable being looped over
    - asyncpg_array_type (str, optional): explicit asyncpg array element type
    """
    method_name = str(context.get("method_name", "execute"))
    query_text = context.get("query_text")
    loop_variable = str(context.get("loop_variable", "item"))
    iterable = str(context.get("iterable", "items"))

    table, column = _extract_table_and_column(query_text)
    table = table or "target_table"
    column = column or "foreign_key_column"
    batch_values_var = f"{column}_values"
    batch_query_raw = f"SELECT * FROM {table} WHERE {column} = ANY(%s)"
    receiver = "conn" if method_name in _ASYNCPG_METHODS else "cursor"

    if method_name in _ASYNCPG_METHODS:
        asyncpg_array_type = str(
            context.get("asyncpg_array_type", _extract_asyncpg_array_type(query_text))
        )
        return (
            "```python\n"
            "# Before (N+1):\n"
            f"for {loop_variable} in {iterable}:\n"
            f'    row = await {receiver}.{method_name}("SELECT * FROM {table} '
            f'WHERE {column} = $1", {loop_variable}.{column})\n'
            "\n"
            "# After (batch):\n"
            f"{batch_values_var} = ["
            f"{loop_variable}.{column} for {loop_variable} in "
            f"{iterable}]\n"
            f'rows = await {receiver}.{method_name}("SELECT * FROM {table} WHERE '
            f'{column} = ANY($1::{asyncpg_array_type}[])", '
            f"{batch_values_var})\n"
            "```"
        )

    return (
        "```python\n"
        "# Before (N+1):\n"
        f"for {loop_variable} in {iterable}:\n"
        f'    row = {receiver}.{method_name}("SELECT * FROM {table} '
        f'WHERE {column} = %s", ({loop_variable}.{column},))\n'
        "\n"
        "# After (batch):\n"
        f"{batch_values_var} = ["
        f"{loop_variable}.{column} for {loop_variable} in "
        f"{iterable}]\n"
        f"rows = {receiver}.{method_name}("
        f'"{batch_query_raw}", ({batch_values_var},))\n'
        "```"
    )


def suggest_sqlalchemy_eager_loading_fix(context: dict[str, Any]) -> str:
    model_name = str(context.get("model_name", "Model"))
    relationship = str(context.get("relationship", "items"))
    iterable = str(context.get("iterable", "results"))
    return (
        "```python\n"
        "# Before (N+1):\n"
        f"{iterable} = session.query({model_name}).all()\n"
        "\n"
        "# After (eager):\n"
        "from sqlalchemy.orm import selectinload\n"
        f"{iterable} = session.query({model_name}).options("
        f"selectinload({model_name}.{relationship})).all()\n"
        "```"
    )
