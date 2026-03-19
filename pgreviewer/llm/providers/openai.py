from __future__ import annotations

import time

from pgreviewer.exceptions import LLMUnavailableError

try:
    import openai as _openai
except ImportError:
    _openai = None  # type: ignore[assignment]


class OpenAIProvider:
    def __init__(
        self,
        api_key: str | None,
        model: str,
        base_url: str | None = None,
    ) -> None:
        if _openai is None:
            raise LLMUnavailableError(
                "OpenAI SDK is not installed. Run: pip install pgreviewer[openai]"
            )
        if not api_key:
            raise LLMUnavailableError(
                "No API key configured for OpenAI. "
                "Set OPENAI_API_KEY (or LLM_API_KEY as fallback)."
            )
        self._model = model
        kwargs: dict = {"api_key": api_key}
        if base_url:
            kwargs["base_url"] = base_url
        self._client = _openai.OpenAI(**kwargs)

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
                response = self._client.chat.completions.create(
                    model=self._model,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = response.choices[0].message.content or ""
                usage = response.usage
                return text, usage.prompt_tokens, usage.completion_tokens
            except _openai.RateLimitError as exc:
                last_error = exc
                if attempt == 2:
                    raise LLMUnavailableError("OpenAI rate limit exceeded") from exc
                time.sleep(2**attempt)
            except _openai.APIError as exc:
                raise LLMUnavailableError(
                    f"OpenAI API error: {type(exc).__name__}: {exc}"
                ) from exc
            except Exception as exc:
                raise LLMUnavailableError(
                    f"OpenAI unexpected error: {type(exc).__name__}: {exc}"
                ) from exc
        raise LLMUnavailableError("LLM generation failed") from last_error
