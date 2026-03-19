from __future__ import annotations

from pgreviewer.exceptions import LLMUnavailableError

try:
    import google.generativeai as _genai
except ImportError:
    _genai = None  # type: ignore[assignment]


class GeminiProvider:
    def __init__(self, api_key: str | None, model: str) -> None:
        if _genai is None:
            raise LLMUnavailableError(
                "Google Generative AI SDK is not installed. "
                "Run: pip install pgreviewer[gemini]"
            )
        if not api_key:
            raise LLMUnavailableError(
                "No API key configured for Gemini. "
                "Set GEMINI_API_KEY (or LLM_API_KEY as fallback)."
            )
        self._model_name = model
        _genai.configure(api_key=api_key)
        self._model = _genai.GenerativeModel(model_name=model)

    @property
    def model_name(self) -> str:
        return self._model_name

    def generate(
        self,
        prompt: str,
        *,
        max_tokens: int,
        temperature: float = 0,
    ) -> tuple[str, int, int]:
        try:
            config = _genai.GenerationConfig(
                max_output_tokens=max_tokens,
                temperature=temperature,
            )
            response = self._model.generate_content(prompt, generation_config=config)
            text = response.text or ""
            usage = response.usage_metadata
            return (
                text,
                getattr(usage, "prompt_token_count", 0),
                getattr(usage, "candidates_token_count", 0),
            )
        except Exception as exc:
            raise LLMUnavailableError(
                f"Gemini API error: {type(exc).__name__}: {exc}"
            ) from exc
