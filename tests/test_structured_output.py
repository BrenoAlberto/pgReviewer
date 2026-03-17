from __future__ import annotations

import pytest
from pydantic import BaseModel

from pgreviewer.exceptions import StructuredOutputError
from pgreviewer.llm.structured_output import generate_structured


class _MockLLMClient:
    def __init__(self, responses: list[str]):
        self._responses = list(responses)
        self.prompts: list[str] = []

    def generate(
        self,
        prompt: str,
        category: str,
        estimated_tokens: int,
    ) -> str:
        del category, estimated_tokens
        self.prompts.append(prompt)
        return self._responses.pop(0)


class _ResponseModel(BaseModel):
    message: str


def test_generate_structured_returns_validated_model() -> None:
    client = _MockLLMClient(['{"message":"hello"}'])

    result = generate_structured(
        client=client,
        prompt="Say hello",
        response_model=_ResponseModel,
        category="interpretation",
        estimated_tokens=100,
    )

    assert result == _ResponseModel(message="hello")
    assert len(client.prompts) == 1
    assert "Respond ONLY with a JSON object matching this schema" in client.prompts[0]


def test_generate_structured_retries_and_recovers() -> None:
    client = _MockLLMClient(["{", '{"message":"hello"}'])

    result = generate_structured(
        client=client,
        prompt="Say hello",
        response_model=_ResponseModel,
        category="interpretation",
        estimated_tokens=100,
    )

    assert result == _ResponseModel(message="hello")
    assert len(client.prompts) == 2
    assert "Error:" in client.prompts[1]


def test_generate_structured_raises_after_retry_limit() -> None:
    client = _MockLLMClient(["{", "{", "{"])

    with pytest.raises(StructuredOutputError):
        generate_structured(
            client=client,
            prompt="Say hello",
            response_model=_ResponseModel,
            category="interpretation",
            estimated_tokens=100,
        )

    assert len(client.prompts) == 3
