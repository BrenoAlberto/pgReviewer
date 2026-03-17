from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, TypeVar

from pydantic import BaseModel

from pgreviewer.config import settings
from pgreviewer.exceptions import LLMUnavailableError
from pgreviewer.infra.cost_guardrail import CostGuardrail
from pgreviewer.infra.debug_store import DebugStore

try:
    import anthropic
except ImportError:  # pragma: no cover
    anthropic = None

if TYPE_CHECKING:
    from typing import Any

T = TypeVar("T")
MODEL_NAME = "claude-sonnet-4-20250514"


class LLMClient:
    def __init__(self) -> None:
        self._api_key = settings.LLM_API_KEY
        self.guardrail = CostGuardrail(
            cost_store_path=settings.COST_STORE_PATH,
            monthly_budget_usd=settings.LLM_MONTHLY_BUDGET_USD,
            category_limits=settings.llm_category_limits,
            cost_per_token=settings.LLM_COST_PER_TOKEN,
        )
        self.debug_store = DebugStore(settings.DEBUG_STORE_PATH)

        if anthropic is None:
            raise LLMUnavailableError("Anthropic SDK is not installed")
        if not self._api_key:
            raise LLMUnavailableError("LLM_API_KEY is not configured")

        self._client = anthropic.Anthropic(api_key=self._api_key)

    def generate(
        self,
        prompt: str,
        category: str,
        estimated_tokens: int,
        response_model: type[T] | None = None,
    ) -> str | T:
        self.guardrail.pre_check(category, estimated_tokens)

        last_error: Exception | None = None
        response = None
        for attempt in range(3):
            try:
                response = self._client.messages.create(
                    model=MODEL_NAME,
                    temperature=0,
                    max_tokens=estimated_tokens,
                    messages=[{"role": "user", "content": prompt}],
                )
                break
            except anthropic.RateLimitError as error:
                last_error = error
                if attempt == 2:
                    raise LLMUnavailableError(
                        "LLM provider rate limit exceeded"
                    ) from error
                time.sleep(2**attempt)
            except anthropic.APIError as error:
                raise LLMUnavailableError("LLM provider API is unavailable") from error

        if response is None:
            raise LLMUnavailableError("LLM generation failed") from last_error

        response_text = self._extract_text(response)
        usage = getattr(response, "usage", None)
        input_tokens = getattr(usage, "input_tokens", 0)
        output_tokens = getattr(usage, "output_tokens", 0)
        actual_cost = (input_tokens + output_tokens) * settings.LLM_COST_PER_TOKEN

        self.guardrail.record(category, actual_cost)

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
    def _extract_text(response: Any) -> str:
        if hasattr(response, "content"):
            parts = []
            for item in response.content:
                if getattr(item, "type", "") == "text":
                    parts.append(getattr(item, "text", ""))
            return "\n".join(part for part in parts if part)
        return ""

    @staticmethod
    def _parse_response(response_text: str, response_model: type[T]) -> T:
        if issubclass(response_model, BaseModel):
            return response_model.model_validate_json(response_text)
        payload = json.loads(response_text)
        return response_model(**payload)
