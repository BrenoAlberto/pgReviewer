from __future__ import annotations

import time

from pgreviewer.exceptions import LLMUnavailableError

try:
    import anthropic as _anthropic
except ImportError:
    _anthropic = None  # type: ignore[assignment]


class AnthropicProvider:
    def __init__(self, api_key: str | None, model: str) -> None:
        if _anthropic is None:
            raise LLMUnavailableError(
                "Anthropic SDK is not installed. Run: pip install pgreviewer[anthropic]"
            )
        if not api_key:
            raise LLMUnavailableError(
                "No API key configured for Anthropic. "
                "Set ANTHROPIC_API_KEY (or LLM_API_KEY as fallback)."
            )
        self._model = model
        self._client = _anthropic.Anthropic(api_key=api_key)

    @property
    def model_name(self) -> str:
        return self._model

    def generate(
        self,
        prompt: str,
        *,
        max_tokens: int,
        temperature: float = 0,
    ) -> tuple[str, int, int]:
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                response = self._client.messages.create(
                    model=self._model,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = "".join(
                    block.text
                    for block in response.content
                    if getattr(block, "type", "") == "text"
                )
                usage = response.usage
                return text, usage.input_tokens, usage.output_tokens
            except _anthropic.RateLimitError as exc:
                last_error = exc
                if attempt == 2:
                    raise LLMUnavailableError("Anthropic rate limit exceeded") from exc
                time.sleep(2**attempt)
            except _anthropic.APIError as exc:
                raise LLMUnavailableError(
                    f"Anthropic API error: {type(exc).__name__}: {exc}"
                ) from exc
            except Exception as exc:
                raise LLMUnavailableError(
                    f"Anthropic unexpected error: {type(exc).__name__}: {exc}"
                ) from exc
        raise LLMUnavailableError("LLM generation failed") from last_error
