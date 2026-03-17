from __future__ import annotations

import json
import logging
from math import ceil
from typing import Any

from pydantic import BaseModel

from pgreviewer.core.models import SchemaInfo
from pgreviewer.llm.client import LLMClient
from pgreviewer.llm.structured_output import generate_structured

logger = logging.getLogger(__name__)

MAX_CONTEXT_TOKENS = 3000
OUTPUT_TOKENS = 900
CHARS_PER_TOKEN = 4


class Bottleneck(BaseModel):
    node_type: str
    relation: str | None = None
    estimated_cost: float | None = None
    details: str


class IndexSuggestion(BaseModel):
    table: str
    columns: list[str]
    rationale: str
    confidence: float


class ExplainInterpretation(BaseModel):
    summary: str
    bottlenecks: list[Bottleneck]
    root_cause: str
    suggested_indexes: list[IndexSuggestion]
    confidence: float


def _estimate_tokens(text: str) -> int:
    # Approximation for prompt budgeting (char length, not model tokenizer exact count).
    return ceil(len(text) / CHARS_PER_TOKEN)


def _collect_referenced_tables(plan: Any) -> set[str]:
    found: set[str] = set()
    stack = [plan]

    while stack:
        value = stack.pop()
        if isinstance(value, dict):
            relation = value.get("Relation Name") or value.get("relation_name")
            if isinstance(relation, str):
                found.add(relation)
            stack.extend(value.values())
        elif isinstance(value, list):
            stack.extend(value)

    return found


def _filter_schema_for_tables(
    schema: SchemaInfo | dict[str, Any], tables: set[str]
) -> dict[str, Any]:
    if isinstance(schema, SchemaInfo):
        table_map = {
            table_name: table_info.model_dump(mode="json")
            for table_name, table_info in schema.tables.items()
            if table_name in tables
        }
        return {"tables": table_map}

    if not isinstance(schema, dict):
        return {"tables": {}}

    tables_map = schema.get("tables")
    if isinstance(tables_map, dict):
        return {
            "tables": {
                table_name: table_info
                for table_name, table_info in tables_map.items()
                if table_name in tables
            }
        }
    return {
        table_name: table_info
        for table_name, table_info in schema.items()
        if table_name in tables
    }


def _filter_table_stats(
    table_stats: dict[str, Any], tables: set[str]
) -> dict[str, Any]:
    return {table: stats for table, stats in table_stats.items() if table in tables}


def _summarize_plan_for_budget(plan: dict[str, Any]) -> dict[str, Any]:
    root = plan.get("Plan", {})
    root_summary: dict[str, Any] = {}
    for key in (
        "Node Type",
        "Relation Name",
        "Total Cost",
        "Startup Cost",
        "Plan Rows",
        "Filter",
        "Index Cond",
        "Hash Cond",
        "Merge Cond",
        "Join Filter",
    ):
        if key in root:
            root_summary[key] = root[key]

    children = root.get("Plans", []) if isinstance(root, dict) else []
    child_summaries: list[dict[str, Any]] = []
    if isinstance(children, list):
        for child in children[:10]:
            if isinstance(child, dict):
                child_summaries.append(
                    {
                        "Node Type": child.get("Node Type"),
                        "Relation Name": child.get("Relation Name"),
                        "Total Cost": child.get("Total Cost"),
                    }
                )

    return {
        "truncated": True,
        "note": "Original EXPLAIN JSON exceeded token budget; summarized for context.",
        "Plan": root_summary,
        "Children": child_summaries,
    }


def build_explain_interpreter_prompt(
    plan: dict[str, Any],
    schema: SchemaInfo | dict[str, Any],
    table_stats: dict[str, Any],
    *,
    max_context_tokens: int = MAX_CONTEXT_TOKENS,
) -> str:
    referenced_tables = _collect_referenced_tables(plan)
    filtered_schema = _filter_schema_for_tables(schema, referenced_tables)
    filtered_table_stats = _filter_table_stats(table_stats, referenced_tables)

    instructions = (
        "You are a PostgreSQL EXPLAIN plan interpreter.\n"
        "Return a JSON object that matches this shape exactly:\n"
        "{\n"
        '  "summary": "1-2 sentence summary",\n'
        '  "bottlenecks": [\n'
        "    {\n"
        '      "node_type": "node type",\n'
        '      "relation": "table or null",\n'
        '      "estimated_cost": 0.0,\n'
        '      "details": "why this node is expensive"\n'
        "    }\n"
        "  ],\n"
        '  "root_cause": "primary cause of slowness",\n'
        '  "suggested_indexes": [\n'
        "    {\n"
        '      "table": "table name",\n'
        '      "columns": ["col_a", "col_b"],\n'
        '      "rationale": "why these columns help",\n'
        '      "confidence": 0.0\n'
        "    }\n"
        "  ],\n"
        '  "confidence": 0.0\n'
        "}\n"
        "Focus on column-level index suggestions and avoid narrative text."
    )

    schema_json = json.dumps(filtered_schema, indent=2, sort_keys=True)
    stats_json = json.dumps(filtered_table_stats, indent=2, sort_keys=True)
    explain_json = json.dumps(plan, indent=2, sort_keys=True)

    static_prompt = (
        "<task>\n"
        f"{instructions}\n"
        "</task>\n\n"
        "<schema>\n"
        f"{schema_json}\n"
        "</schema>\n\n"
        "<table_stats>\n"
        f"{stats_json}\n"
        "</table_stats>\n\n"
    )

    available_tokens_for_explain = max(
        0, max_context_tokens - _estimate_tokens(static_prompt)
    )
    if _estimate_tokens(explain_json) > available_tokens_for_explain:
        summarized = _summarize_plan_for_budget(plan)
        explain_json = json.dumps(summarized, indent=2, sort_keys=True)
        if _estimate_tokens(explain_json) > available_tokens_for_explain:
            explain_json = json.dumps(
                {
                    "truncated": True,
                    "note": "EXPLAIN JSON omitted because token budget is too small.",
                },
                indent=2,
                sort_keys=True,
            )
        logger.warning(
            (
                "EXPLAIN context exceeded token budget; "
                "truncated plan JSON to approximately %s tokens"
            ),
            max_context_tokens,
        )

    return f"{static_prompt}<explain_plan>\n{explain_json}\n</explain_plan>"


def interpret_explain(
    plan: dict[str, Any],
    schema: SchemaInfo | dict[str, Any],
    table_stats: dict[str, Any],
    *,
    client: LLMClient | None = None,
) -> ExplainInterpretation:
    llm_client = client or LLMClient()
    prompt = build_explain_interpreter_prompt(plan, schema, table_stats)
    return generate_structured(
        client=llm_client,
        prompt=prompt,
        response_model=ExplainInterpretation,
        category="interpretation",
        estimated_tokens=OUTPUT_TOKENS,
    )
