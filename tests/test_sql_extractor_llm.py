from __future__ import annotations

import json

from pgreviewer.llm.prompts.sql_extractor import (
    OUTPUT_TOKENS,
    SQLExtractionResult,
    build_sql_extractor_prompt,
    extract_sql_with_llm,
    map_to_extracted_queries,
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
        assert category == "extraction"
        assert estimated_tokens == OUTPUT_TOKENS
        self.prompts.append(prompt)
        return self.response


def test_build_sql_extractor_prompt_includes_code_and_file_context() -> None:
    prompt = build_sql_extractor_prompt(
        "cursor.execute(f'SELECT * FROM users WHERE id = {user_id}')",
        file_context="app/repositories/user_repository.py:88",
    )

    assert "<file_context>" in prompt
    assert "app/repositories/user_repository.py:88" in prompt
    assert "<code>" in prompt
    assert "cursor.execute" in prompt
    assert "string concatenation/query-builder patterns" in prompt
    assert "For f-string SQL, extract the SQL template first" in prompt


def test_extract_sql_with_llm_returns_structured_result() -> None:
    client = _MockLLMClient(
        json.dumps(
            {
                "queries": [
                    {
                        "sql": "SELECT * FROM users WHERE id = :user_id",
                        "confidence": 0.72,
                        "notes": "f-string template",
                    }
                ]
            }
        )
    )

    result = extract_sql_with_llm(
        "cursor.execute(f'SELECT * FROM users WHERE id = {user_id}')",
        file_context="src/user_repo.py:12",
        client=client,
    )

    assert isinstance(result, SQLExtractionResult)
    assert result.queries[0].sql == "SELECT * FROM users WHERE id = :user_id"
    assert result.queries[0].confidence == 0.72
    assert len(client.prompts) == 1
    assert "src/user_repo.py:12" in client.prompts[0]


def test_map_to_extracted_queries_preserves_llm_confidence_and_notes() -> None:
    result = SQLExtractionResult.model_validate(
        {
            "queries": [
                {
                    "sql": "SELECT * FROM users WHERE id = :user_id",
                    "confidence": 0.72,
                    "notes": "f-string template",
                },
            ]
        }
    )

    mapped = map_to_extracted_queries(
        result,
        source_file="src/user_repo.py",
        line_number=12,
    )

    assert len(mapped) == 1
    assert mapped[0].extraction_method == "llm"
    assert mapped[0].confidence == 0.72
    assert mapped[0].notes == "f-string template"


def test_map_to_extracted_queries_substitutes_params_for_dynamic_where_clause() -> None:
    result = SQLExtractionResult.model_validate(
        {
            "queries": [
                {
                    "sql": "SELECT * FROM orders WHERE 1=1 AND status = %s",
                    "confidence": 0.63,
                    "notes": "dynamic WHERE clause",
                }
            ]
        }
    )

    mapped = map_to_extracted_queries(
        result,
        source_file="src/orders_repo.py",
        line_number=18,
    )

    assert len(mapped) == 1
    assert mapped[0].sql == "SELECT * FROM orders WHERE 1=1 AND status = 'placeholder'"
    assert "dynamic WHERE clause" in (mapped[0].notes or "")
    assert "parameterized query" in (mapped[0].notes or "")


def test_map_to_extracted_queries_substitutes_fstring_template_using_context() -> None:
    result = SQLExtractionResult.model_validate(
        {
            "queries": [
                {
                    "sql": "SELECT * FROM {table} WHERE user_id = {user_id}",
                    "confidence": 0.82,
                    "notes": "f-string template",
                }
            ]
        }
    )

    source_context = """
from models import Order

def load(cursor, table, user_id):
    cursor.execute(f"SELECT * FROM {table} WHERE user_id = {user_id}")
"""
    mapped = map_to_extracted_queries(
        result,
        source_file="src/orders_repo.py",
        line_number=12,
        source_context=source_context,
    )

    assert len(mapped) == 1
    assert mapped[0].sql == "SELECT * FROM orders WHERE user_id = 42"
    assert mapped[0].confidence == 0.65
    assert "f-string: table='{table}' substituted from context" in (
        mapped[0].notes or ""
    )


def test_map_to_extracted_queries_substitutes_non_id_fstring_values() -> None:
    result = SQLExtractionResult.model_validate(
        {
            "queries": [
                {
                    "sql": "SELECT * FROM {table_name} WHERE {column} = {value}",
                    "confidence": 0.81,
                    "notes": "f-string template",
                }
            ]
        }
    )
    mapped = map_to_extracted_queries(
        result,
        source_file="src/orders_repo.py",
        line_number=33,
        source_context="from models import Order",
    )

    assert len(mapped) == 1
    assert mapped[0].sql == "SELECT * FROM orders WHERE id = 'placeholder'"
    assert mapped[0].confidence == 0.65
    assert "f-string: table='{table_name}' substituted from context" in (
        mapped[0].notes or ""
    )
