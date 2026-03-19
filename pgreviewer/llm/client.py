from __future__ import annotations

import json
from typing import TYPE_CHECKING, TypeVar

from pydantic import BaseModel

from pgreviewer.config import settings
from pgreviewer.infra.cost_guardrail import CostGuardrail
from pgreviewer.infra.debug_store import DebugStore
from pgreviewer.llm.pricing import cost_for_call, estimate_cost

if TYPE_CHECKING:
    from pgreviewer.llm.provider import LLMProvider

T = TypeVar("T")

# Accumulates LLM cost and tracks the model used across all LLMClient instances
# within a single CLI run.
_run_cost_usd: float = 0.0
_run_model: str | None = None


def get_run_cost_usd() -> float:
    return _run_cost_usd


def get_run_model() -> str | None:
    return _run_model


def reset_run_cost() -> None:
    global _run_cost_usd, _run_model
    _run_cost_usd = 0.0
    _run_model = None


class LLMClient:
    def __init__(self, provider: LLMProvider | None = None) -> None:
        if provider is None:
            from pgreviewer.llm.providers import build_provider

            provider = build_provider(settings)
        self._provider = provider
        self.guardrail = CostGuardrail(
            cost_store_path=settings.COST_STORE_PATH,
            monthly_budget_usd=settings.LLM_MONTHLY_BUDGET_USD,
            category_limits=settings.llm_category_limits,
        )
        self.debug_store = DebugStore(settings.DEBUG_STORE_PATH)

    def generate(
        self,
        prompt: str,
        category: str,
        estimated_tokens: int,
        response_model: type[T] | None = None,
    ) -> str | T:
        estimated_cost = estimate_cost(self._provider.model_name, estimated_tokens)
        self.guardrail.pre_check(category, estimated_cost)

        response_text, input_tokens, output_tokens = self._provider.generate(
            prompt, max_tokens=estimated_tokens
        )
        actual_cost = cost_for_call(
            self._provider.model_name, input_tokens, output_tokens
        )

        self.guardrail.record(category, actual_cost)

        global _run_cost_usd, _run_model
        _run_cost_usd += actual_cost
        _run_model = self._provider.model_name

        run_id = self.debug_store.new_run_id()
        self.debug_store.save(
            run_id,
            category,
            {
                "prompt": prompt,
                "response": response_text,
                "cost": actual_cost,
            },
        )

        if response_model is None:
            return response_text
        return self._parse_response(response_text, response_model)

    @staticmethod
    def _parse_response(response_text: str, response_model: type[T]) -> T:
        if issubclass(response_model, BaseModel):
            return response_model.model_validate_json(response_text)
        payload = json.loads(response_text)
        return response_model(**payload)
