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
                {
                    "sql": "SELECT * FROM users",
                    "confidence": 0.45,
                    "notes": "dynamic WHERE clause",
                },
            ]
        }
    )

    mapped = map_to_extracted_queries(
        result,
        source_file="src/user_repo.py",
        line_number=12,
    )

    assert len(mapped) == 2
    assert mapped[0].extraction_method == "llm"
    assert mapped[0].confidence == 0.72
    assert mapped[0].notes == "f-string template"
    assert mapped[1].confidence == 0.45
    assert mapped[1].notes == "dynamic WHERE clause"
