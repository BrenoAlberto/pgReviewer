from __future__ import annotations

import pytest
from pydantic import BaseModel

from pgreviewer.exceptions import BudgetExceededError, LLMUnavailableError
from pgreviewer.llm import client as client_module
from pgreviewer.llm.client import LLMClient

# ---------------------------------------------------------------------------
# Fake provider
# ---------------------------------------------------------------------------


class _FakeProvider:
    """Stub LLMProvider for unit tests — no real SDK calls."""

    def __init__(self, responses: list):
        self._responses = list(responses)
        self.calls = 0
        self._model = "claude-sonnet-4-5"

    @property
    def model_name(self) -> str:
        return self._model

    def generate(self, prompt: str, *, max_tokens: int, temperature: float = 0):
        self.calls += 1
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        text, input_tokens, output_tokens = item
        return text, input_tokens, output_tokens


def _resp(text: str, input_tokens: int = 10, output_tokens: int = 10):
    return (text, input_tokens, output_tokens)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def llm_config(monkeypatch, tmp_path):
    monkeypatch.setattr(client_module.settings, "DEBUG_STORE_PATH", tmp_path / "debug")
    monkeypatch.setattr(client_module.settings, "COST_STORE_PATH", tmp_path / "costs")
    monkeypatch.setattr(client_module.settings, "LLM_MONTHLY_BUDGET_USD", 10.0)
    monkeypatch.setattr(client_module.settings, "LLM_BUDGET_INTERPRETATION", 1.0)
    monkeypatch.setattr(client_module.settings, "LLM_BUDGET_EXTRACTION", 0.0)
    monkeypatch.setattr(client_module.settings, "LLM_BUDGET_REPORTING", 0.0)
    return tmp_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_generate_returns_string_and_stores_debug_artifact(llm_config):
    provider = _FakeProvider([_resp("hello")])
    client = LLMClient(provider=provider)

    result = client.generate(
        "Say hello", category="interpretation", estimated_tokens=100
    )

    assert result == "hello"
    artifacts = list((llm_config / "debug").glob("*/*/interpretation.json"))
    assert len(artifacts) == 1
    payload = artifacts[0].read_text()
    assert '"prompt": "Say hello"' in payload
    assert '"response": "hello"' in payload


def test_generate_raises_budget_exceeded_before_provider_call(monkeypatch, llm_config):
    # Make the budget tiny so any call exceeds it
    monkeypatch.setattr(client_module.settings, "LLM_MONTHLY_BUDGET_USD", 0.000001)
    provider = _FakeProvider([_resp("unused")])
    client = LLMClient(provider=provider)

    with pytest.raises(BudgetExceededError):
        client.generate("Say hello", category="interpretation", estimated_tokens=1000)

    assert provider.calls == 0


def test_generate_propagates_llm_unavailable_error(llm_config):
    provider = _FakeProvider([LLMUnavailableError("rate limit")])
    client = LLMClient(provider=provider)

    with pytest.raises(LLMUnavailableError):
        client.generate("Say hello", category="interpretation", estimated_tokens=100)


class _ResponseModel(BaseModel):
    message: str


def test_generate_returns_response_model(llm_config):
    provider = _FakeProvider([_resp('{"message":"hello"}')])
    client = LLMClient(provider=provider)

    result = client.generate(
        "Say hello",
        category="interpretation",
        estimated_tokens=100,
        response_model=_ResponseModel,
    )

    assert result == _ResponseModel(message="hello")


def test_run_cost_accumulates_across_calls(llm_config):
    client_module.reset_run_cost()
    provider = _FakeProvider([_resp("a", 100, 50), _resp("b", 200, 100)])
    client = LLMClient(provider=provider)

    client.generate("p1", category="interpretation", estimated_tokens=200)
    client.generate("p2", category="interpretation", estimated_tokens=200)

    assert client_module.get_run_cost_usd() > 0


def test_run_model_tracks_provider_model(llm_config):
    client_module.reset_run_cost()
    provider = _FakeProvider([_resp("hello")])
    client = LLMClient(provider=provider)

    assert client_module.get_run_model() is None  # before any call

    client.generate("p1", category="interpretation", estimated_tokens=100)

    assert client_module.get_run_model() == "claude-sonnet-4-5"


def test_reset_clears_model(llm_config):
    client_module.reset_run_cost()
    provider = _FakeProvider([_resp("hello")])
    client = LLMClient(provider=provider)

    client.generate("p1", category="interpretation", estimated_tokens=100)
    client_module.reset_run_cost()

    assert client_module.get_run_model() is None
