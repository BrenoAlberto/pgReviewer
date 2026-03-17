from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from pgreviewer.analysis.complexity_router import should_use_llm
from pgreviewer.analysis.plan_parser import parse_explain
from pgreviewer.core.models import ColumnInfo, Issue, SchemaInfo, Severity, TableInfo
from pgreviewer.llm.prompts.explain_interpreter import (
    ExplainInterpretation,
    IndexSuggestion,
    build_explain_interpreter_prompt,
)

_EXPLAIN_FIXTURES_DIR = Path(__file__).parents[1] / "fixtures" / "explain"
_COMPLEX_FIXTURES_DIR = _EXPLAIN_FIXTURES_DIR / "complex"


def _load_raw_plan(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))[0]


def _load_parsed_plan(path: Path):
    return parse_explain(_load_raw_plan(path))


def _issue(confidence: float = 1.0) -> Issue:
    return Issue(
        severity=Severity.WARNING,
        detector_name="test_detector",
        description="detector finding",
        affected_table="orders",
        affected_columns=["user_id"],
        suggested_action="inspect manually",
        confidence=confidence,
    )


@pytest.mark.parametrize(
    ("fixture_name", "issues", "expected"),
    [
        ("three_table_join.json", [], (True, "3+ joins")),
        ("cte_with_subquery.json", [], (True, "contains CTE")),
        (
            "high_cost_no_clear_fix.json",
            [_issue(1.0)],
            (True, "high cost with few detector hits"),
        ),
    ],
)
def test_complexity_router_routes_complex_fixture_plans(
    fixture_name: str,
    issues: list[Issue],
    expected: tuple[bool, str],
) -> None:
    plan = _load_parsed_plan(_COMPLEX_FIXTURES_DIR / fixture_name)

    assert should_use_llm(plan, issues) == expected


def test_complexity_router_routes_simple_fixture_plan_to_non_llm() -> None:
    plan = _load_parsed_plan(_EXPLAIN_FIXTURES_DIR / "seq_scan_small.json")

    assert should_use_llm(plan, []) == (False, "simple plan")


def test_explain_interpreter_prompt_is_well_formed_and_schema_filtered() -> None:
    plan = _load_raw_plan(_COMPLEX_FIXTURES_DIR / "three_table_join.json")
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
            "payments": TableInfo(
                row_estimate=30000,
                columns=[ColumnInfo(name="id", type="integer")],
            ),
        }
    )

    prompt = build_explain_interpreter_prompt(
        plan,
        schema,
        {
            "users": {"row_count": 1000},
            "orders": {"row_count": 5000},
            "products": {"row_count": 200},
            "payments": {"row_count": 30000},
        },
    )

    assert "<schema>" in prompt
    assert "<explain_plan>" in prompt
    assert "users" in prompt and "orders" in prompt and "products" in prompt
    assert "payments" not in prompt


@pytest.fixture
def llm_interpretation_fixture() -> ExplainInterpretation:
    return ExplainInterpretation(
        summary="Complex plan with expensive joins",
        bottlenecks=[],
        root_cause="Missing selective indexes",
        suggested_indexes=[
            IndexSuggestion(
                table="orders",
                columns=["user_id"],
                rationale="join key",
                confidence=0.95,
            ),
            IndexSuggestion(
                table="orders",
                columns=["product_id"],
                rationale="second join key",
                confidence=0.75,
            ),
            IndexSuggestion(
                table="orders",
                columns=["status"],
                rationale="filter column",
                confidence=0.45,
            ),
        ],
        confidence=0.9,
    )


@pytest.mark.asyncio
async def test_downstream_calls_hypopg_validation_for_each_llm_suggestion(
    llm_interpretation_fixture: ExplainInterpretation,
) -> None:
    from pgreviewer.cli.commands.check import _analyse_query
    from pgreviewer.core.models import TableInfo

    backend = AsyncMock()
    backend.get_explain_plan.side_effect = [
        _load_raw_plan(_COMPLEX_FIXTURES_DIR / "three_table_join.json"),
        {"Plan": {"Total Cost": 700.0}},
        {"Plan": {"Total Cost": 650.0}},
        {"Plan": {"Total Cost": 120000.0}},
    ]
    backend.recommend_indexes.return_value = []
    backend.get_schema_info.return_value = TableInfo()

    with (
        patch("pgreviewer.db.pool.close_pool") as mock_close,
        patch("pgreviewer.core.backend.get_backend", return_value=backend),
        patch("pgreviewer.analysis.plan_parser.parse_explain") as mock_parse,
        patch("pgreviewer.analysis.plan_parser.extract_tables") as mock_extract,
        patch("pgreviewer.analysis.issue_detectors.run_all_detectors") as mock_detect,
        patch(
            "pgreviewer.analysis.complexity_router.should_use_llm",
            return_value=(True, "complex query"),
        ),
        patch(
            "pgreviewer.llm.prompts.explain_interpreter.interpret_explain",
            return_value=llm_interpretation_fixture,
        ),
        patch("pgreviewer.llm.client.LLMClient"),
        patch(
            "pgreviewer.analysis.index_generator.generate_create_index",
            side_effect=lambda rec: (
                f"CREATE INDEX ON {rec.table}({', '.join(rec.columns)})"
            ),
        ),
    ):
        mock_parse.return_value = _load_parsed_plan(
            _COMPLEX_FIXTURES_DIR / "three_table_join.json"
        )
        mock_extract.return_value = ["orders", "users", "products"]
        mock_detect.return_value = []

        with patch("pgreviewer.config.settings.LLM_API_KEY", "test-key"):
            result = await _analyse_query("SELECT * FROM orders")

    assert backend.get_explain_plan.await_count == 1 + len(
        llm_interpretation_fixture.suggested_indexes
    )
    assert len(result.recommendations) == 2
    assert {rec.confidence for rec in result.recommendations} == {0.95, 0.75}
    mock_close.assert_called_once()


def test_downstream_confidence_thresholds_render_expected_sections(capsys) -> None:
    from pgreviewer.cli.commands.check import _print_recommendations
    from pgreviewer.core.models import IndexRecommendation

    _print_recommendations(
        [
            IndexRecommendation(
                table="orders",
                columns=["user_id"],
                create_statement="CREATE INDEX ON orders(user_id)",
                cost_before=100.0,
                cost_after=60.0,
                improvement_pct=0.4,
                validated=True,
                confidence=0.9,
            ),
            IndexRecommendation(
                table="orders",
                columns=["product_id"],
                create_statement="CREATE INDEX ON orders(product_id)",
                cost_before=100.0,
                cost_after=90.0,
                improvement_pct=0.1,
                validated=False,
                confidence=0.75,
            ),
            IndexRecommendation(
                table="orders",
                columns=["status"],
                create_statement="CREATE INDEX ON orders(status)",
                validated=False,
                confidence=0.5,
            ),
        ]
    )

    output = capsys.readouterr().out
    assert "Recommended Indexes" in output
    assert "moderate confidence — verify before applying" in output
    assert "Possible issues (low confidence)" in output
