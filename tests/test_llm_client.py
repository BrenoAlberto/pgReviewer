from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import BaseModel

from pgreviewer.exceptions import BudgetExceededError, LLMUnavailableError
from pgreviewer.llm import client as client_module
from pgreviewer.llm.client import LLMClient


class _FakeRateLimitError(Exception):
    pass


class _FakeAPIError(Exception):
    pass


class _FakeMessagesAPI:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    def create(self, **kwargs):
        self.calls += 1
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class _FakeAnthropicClient:
    def __init__(self, responses):
        self.messages = _FakeMessagesAPI(responses)


def _fake_text_response(text: str, input_tokens: int = 10, output_tokens: int = 10):
    return SimpleNamespace(
        content=[SimpleNamespace(type="text", text=text)],
        usage=SimpleNamespace(input_tokens=input_tokens, output_tokens=output_tokens),
    )


def _build_fake_anthropic(responses):
    return SimpleNamespace(
        RateLimitError=_FakeRateLimitError,
        APIError=_FakeAPIError,
        Anthropic=lambda api_key: _FakeAnthropicClient(responses=responses),
    )


@pytest.fixture()
def llm_config(monkeypatch, tmp_path):
    monkeypatch.setattr(client_module.settings, "LLM_API_KEY", "test-key")
    monkeypatch.setattr(client_module.settings, "DEBUG_STORE_PATH", tmp_path / "debug")
    monkeypatch.setattr(client_module.settings, "COST_STORE_PATH", tmp_path / "costs")
    monkeypatch.setattr(client_module.settings, "LLM_MONTHLY_BUDGET_USD", 10.0)
    monkeypatch.setattr(client_module.settings, "LLM_BUDGET_INTERPRETATION", 1.0)
    monkeypatch.setattr(client_module.settings, "LLM_BUDGET_EXTRACTION", 0.0)
    monkeypatch.setattr(client_module.settings, "LLM_BUDGET_REPORTING", 0.0)
    monkeypatch.setattr(client_module.settings, "LLM_COST_PER_TOKEN", 0.01)
    return tmp_path


def test_generate_returns_string_and_stores_debug_artifact(monkeypatch, llm_config):
    monkeypatch.setattr(
        client_module,
        "anthropic",
        _build_fake_anthropic([_fake_text_response("hello")]),
    )
    client = LLMClient()

    result = client.generate(
        "Say hello", category="interpretation", estimated_tokens=100
    )

    assert result == "hello"
    artifacts = list((llm_config / "debug").glob("*/*/interpretation.json"))
    assert len(artifacts) == 1
    payload = artifacts[0].read_text()
    assert '"prompt": "Say hello"' in payload
    assert '"response": "hello"' in payload


def test_generate_raises_budget_exceeded_before_sdk_call(monkeypatch, llm_config):
    monkeypatch.setattr(
        client_module,
        "anthropic",
        _build_fake_anthropic([_fake_text_response("unused")]),
    )
    monkeypatch.setattr(client_module.settings, "LLM_MONTHLY_BUDGET_USD", 0.1)
    monkeypatch.setattr(client_module.settings, "LLM_COST_PER_TOKEN", 1.0)
    client = LLMClient()

    with pytest.raises(BudgetExceededError):
        client.generate("Say hello", category="interpretation", estimated_tokens=1)

    assert client._client.messages.calls == 0


def test_generate_retries_rate_limit_errors(monkeypatch, llm_config):
    monkeypatch.setattr(
        client_module,
        "anthropic",
        _build_fake_anthropic(
            [_FakeRateLimitError(), _FakeRateLimitError(), _fake_text_response("ok")]
        ),
    )
    sleep_calls: list[int] = []
    monkeypatch.setattr(
        client_module.time, "sleep", lambda seconds: sleep_calls.append(seconds)
    )
    client = LLMClient()

    result = client.generate(
        "Say hello", category="interpretation", estimated_tokens=100
    )

    assert result == "ok"
    assert client._client.messages.calls == 3
    assert sleep_calls == [1, 2]


def test_generate_maps_api_error(monkeypatch, llm_config):
    monkeypatch.setattr(
        client_module, "anthropic", _build_fake_anthropic([_FakeAPIError()])
    )
    client = LLMClient()

    with pytest.raises(LLMUnavailableError):
        client.generate("Say hello", category="interpretation", estimated_tokens=100)


class _ResponseModel(BaseModel):
    message: str


def test_generate_returns_response_model(monkeypatch, llm_config):
    monkeypatch.setattr(
        client_module,
        "anthropic",
        _build_fake_anthropic([_fake_text_response('{"message":"hello"}')]),
    )
    client = LLMClient()

    result = client.generate(
        "Say hello",
        category="interpretation",
        estimated_tokens=100,
        response_model=_ResponseModel,
    )

    assert result == _ResponseModel(message="hello")
