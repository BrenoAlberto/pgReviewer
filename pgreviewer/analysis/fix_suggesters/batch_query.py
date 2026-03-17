from __future__ import annotations

import re
from typing import Any

_TABLE_RE = re.compile(r"\bfrom\s+([a-zA-Z_][\w.]*)", re.IGNORECASE)
_COLUMN_RE = re.compile(
    r"\bwhere\s+([a-zA-Z_][\w.]*)\s*=\s*(?:%s|\$\d+|:[a-zA-Z_]\w*|\?)",
    re.IGNORECASE,
)
_ASYNCPG_METHODS = frozenset({"fetch", "fetchrow", "fetchval"})


def _extract_table_and_column(query_text: str | None) -> tuple[str | None, str | None]:
    if not query_text:
        return None, None
    table_match = _TABLE_RE.search(query_text)
    column_match = _COLUMN_RE.search(query_text)
    table = table_match.group(1) if table_match else None
    column = column_match.group(1) if column_match else None
    return table, column


def suggest_batch_query_fix(context: dict[str, Any]) -> str:
    method_name = str(context.get("method_name", "execute"))
    query_text = context.get("query_text")
    loop_variable = str(context.get("loop_variable", "item"))
    iterable = str(context.get("iterable", "items"))

    table, column = _extract_table_and_column(query_text)
    table = table or "records"
    column = column or "id"
    ids_variable = f"{column}_values"
    batch_query_raw = f"SELECT * FROM {table} WHERE {column} = ANY(%s)"
    batch_query_asyncpg = f"SELECT * FROM {table} WHERE {column} = ANY($1::int[])"
    receiver = "conn" if method_name in _ASYNCPG_METHODS else "cursor"

    if method_name in _ASYNCPG_METHODS:
        return (
            "```python\n"
            "# Before (N+1):\n"
            f"for {loop_variable} in {iterable}:\n"
            f'    row = await {receiver}.{method_name}("SELECT * FROM {table} '
            f'WHERE {column} = $1", {loop_variable}.{column})\n'
            "\n"
            "# After (batch):\n"
            f"{ids_variable} = [{loop_variable}.{column} for {loop_variable} in "
            f"{iterable}]\n"
            f'rows = await {receiver}.{method_name}("{batch_query_asyncpg}", '
            f"{ids_variable})\n"
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
        f"{ids_variable} = [{loop_variable}.{column} for {loop_variable} in "
        f"{iterable}]\n"
        f'rows = {receiver}.{method_name}("{batch_query_raw}", ({ids_variable},))\n'
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
