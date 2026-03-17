from __future__ import annotations

import json
import logging

from pgreviewer.core.models import ColumnInfo, IndexInfo, SchemaInfo, TableInfo
from pgreviewer.llm.prompts.explain_interpreter import (
    ExplainInterpretation,
    build_explain_interpreter_prompt,
    interpret_explain,
)


class _MockLLMClient:
    def __init__(self, response: str) -> None:
        self.response = response
        self.prompts: list[str] = []

    def generate(
        self,
        prompt: str,
        category: str,
        estimated_tokens: int,
    ) -> str:
        self.prompts.append(prompt)
        assert category == "interpretation"
        assert estimated_tokens == 900
        return self.response


def _three_table_join_plan() -> dict:
    return {
        "Plan": {
            "Node Type": "Hash Join",
            "Total Cost": 9500.0,
            "Plans": [
                {
                    "Node Type": "Hash Join",
                    "Total Cost": 7000.0,
                    "Plans": [
                        {
                            "Node Type": "Seq Scan",
                            "Relation Name": "orders",
                            "Total Cost": 3000.0,
                        },
                        {
                            "Node Type": "Seq Scan",
                            "Relation Name": "users",
                            "Total Cost": 2000.0,
                        },
                    ],
                },
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "line_items",
                    "Total Cost": 2500.0,
                },
            ],
        }
    }


def test_build_prompt_uses_xml_tags_and_filters_schema_tables() -> None:
    plan = _three_table_join_plan()
    schema = SchemaInfo(
        tables={
            "users": TableInfo(
                row_estimate=12000,
                indexes=[IndexInfo(name="users_pkey", columns=["id"], is_unique=True)],
                columns=[ColumnInfo(name="id", type="integer")],
            ),
            "orders": TableInfo(
                row_estimate=250000,
                indexes=[IndexInfo(name="orders_pkey", columns=["id"], is_unique=True)],
                columns=[
                    ColumnInfo(name="id", type="integer"),
                    ColumnInfo(name="user_id", type="integer"),
                ],
            ),
            "line_items": TableInfo(
                row_estimate=650000,
                indexes=[
                    IndexInfo(name="line_items_pkey", columns=["id"], is_unique=True)
                ],
                columns=[
                    ColumnInfo(name="id", type="integer"),
                    ColumnInfo(name="order_id", type="integer"),
                ],
            ),
            "audit_logs": TableInfo(
                row_estimate=100,
                indexes=[],
                columns=[ColumnInfo(name="id", type="integer")],
            ),
        }
    )
    table_stats = {
        "users": {"row_count": 12000},
        "orders": {"row_count": 250000},
        "line_items": {"row_count": 650000},
        "audit_logs": {"row_count": 100},
    }

    prompt = build_explain_interpreter_prompt(plan, schema, table_stats)

    assert "<task>" in prompt
    assert "<schema>" in prompt
    assert "<table_stats>" in prompt
    assert "<explain_plan>" in prompt
    assert "audit_logs" not in prompt
    assert "users" in prompt
    assert "orders" in prompt
    assert "line_items" in prompt


def test_build_prompt_truncates_explain_when_over_token_budget(caplog) -> None:
    plan = {
        "Plan": {"Node Type": "Seq Scan", "Relation Name": "users", "blob": "x" * 20000}
    }

    with caplog.at_level(logging.WARNING):
        prompt = build_explain_interpreter_prompt(
            plan,
            {"tables": {"users": {"columns": [], "indexes": []}}},
            {"users": {"row_count": 1000}},
            max_context_tokens=200,
        )

    assert "[TRUNCATED to fit token budget]" in prompt
    assert "truncated plan JSON" in caplog.text


def test_interpret_explain_returns_structured_model_for_three_table_join() -> None:
    client = _MockLLMClient(
        json.dumps(
            {
                "summary": "The query is slow due to full scans across joined tables.",
                "bottlenecks": [
                    {
                        "node_type": "Seq Scan",
                        "relation": "orders",
                        "estimated_cost": 3000.0,
                        "details": "Scanning many rows before join filtering.",
                    }
                ],
                "root_cause": (
                    "Missing join/filter indexes on high-cardinality foreign keys."
                ),
                "suggested_indexes": [
                    {
                        "table": "orders",
                        "columns": ["user_id"],
                        "rationale": "Speeds up users-orders join key lookups.",
                        "confidence": 0.89,
                    },
                    {
                        "table": "line_items",
                        "columns": ["order_id"],
                        "rationale": (
                            "Reduces full scans during orders-line_items join."
                        ),
                        "confidence": 0.9,
                    },
                ],
                "confidence": 0.91,
            }
        )
    )

    result = interpret_explain(
        _three_table_join_plan(),
        {"tables": {"users": {}, "orders": {}, "line_items": {}, "audit_logs": {}}},
        {"users": 1000, "orders": 5000, "line_items": 8000, "audit_logs": 20},
        client=client,
    )

    assert isinstance(result, ExplainInterpretation)
    assert result.suggested_indexes
    assert result.suggested_indexes[0].columns == ["user_id"]
    assert len(client.prompts) == 1
