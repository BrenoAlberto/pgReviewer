from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pgreviewer.core.models import ColumnInfo, SchemaInfo, TableInfo
from pgreviewer.llm.client import LLMClient
from pgreviewer.llm.prompts.explain_interpreter import (
    ExplainInterpretation,
    interpret_explain,
)

_FIXTURE = (
    Path(__file__).parents[1]
    / "fixtures"
    / "explain"
    / "complex"
    / "three_table_join.json"
)


@pytest.mark.llm
@pytest.mark.skipif(
    not os.getenv("LLM_API_KEY"),
    reason="LLM_API_KEY is required for live LLM integration tests",
)
def test_interpret_explain_live_llm_for_fixture_plan() -> None:
    raw_plan = json.loads(_FIXTURE.read_text(encoding="utf-8"))[0]
    schema = SchemaInfo(
        tables={
            "users": TableInfo(
                row_estimate=1000,
                columns=[ColumnInfo(name="id", type="integer")],
            ),
            "orders": TableInfo(
                row_estimate=5000,
                columns=[
                    ColumnInfo(name="id", type="integer"),
                    ColumnInfo(name="user_id", type="integer"),
                    ColumnInfo(name="product_id", type="integer"),
                ],
            ),
            "products": TableInfo(
                row_estimate=200,
                columns=[ColumnInfo(name="id", type="integer")],
            ),
        }
    )

    result = interpret_explain(
        raw_plan,
        schema,
        {
            "users": {"row_count": 1000},
            "orders": {"row_count": 5000},
            "products": {"row_count": 200},
        },
        client=LLMClient(),
    )

    assert isinstance(result, ExplainInterpretation)
    assert result.summary
