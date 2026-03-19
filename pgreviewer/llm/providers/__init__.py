"""Provider factory — resolves LLM_PROVIDER + LLM_MODEL to a concrete adapter."""

from __future__ import annotations

from pgreviewer.exceptions import ConfigError

_PROVIDER_DEFAULT_MODELS: dict[str, str] = {
    "anthropic": "claude-sonnet-4-5",
    "openai": "gpt-4o",
    "gemini": "gemini-2.0-flash",
}


def _infer_provider(model: str) -> str | None:
    """Return the provider name inferred from a well-known model prefix, or None."""
    m = model.lower()
    if m.startswith("claude"):
        return "anthropic"
    if m.startswith(("gpt-", "o1", "o3", "o4")):
        return "openai"
    if m.startswith("gemini"):
        return "gemini"
    return None


def build_provider(settings):
    """Instantiate and return the configured :class:`~pgreviewer.llm.provider.LLMProvider`.

    Resolution order
    ----------------
    1. If ``LLM_MODEL`` is set and its prefix matches a known provider, that
       provider is used regardless of ``LLM_PROVIDER``.
    2. Otherwise ``LLM_PROVIDER`` (default: ``anthropic``) is used.
    3. ``LLM_MODEL`` defaults to the provider's canonical model when not set.
    4. API key: provider-specific env var (``ANTHROPIC_API_KEY``, etc.) takes
       precedence; ``LLM_API_KEY`` is the fallback for single-key setups.
    """  # noqa
    provider_name = settings.LLM_PROVIDER.lower()
    model = settings.LLM_MODEL

    if model:
        inferred = _infer_provider(model)
        if inferred:
            provider_name = inferred

    model = model or _PROVIDER_DEFAULT_MODELS.get(provider_name, "claude-sonnet-4-5")

    def _key(specific: str | None) -> str | None:
        return specific or settings.LLM_API_KEY

    if provider_name == "anthropic":
        from pgreviewer.llm.providers.anthropic import AnthropicProvider

        return AnthropicProvider(api_key=_key(settings.ANTHROPIC_API_KEY), model=model)

    if provider_name == "openai":
        from pgreviewer.llm.providers.openai import OpenAIProvider

        return OpenAIProvider(
            api_key=_key(settings.OPENAI_API_KEY),
            model=model,
            base_url=settings.OPENAI_BASE_URL,
        )

    if provider_name == "gemini":
        from pgreviewer.llm.providers.gemini import GeminiProvider

        return GeminiProvider(api_key=_key(settings.GEMINI_API_KEY), model=model)

    raise ConfigError(
        f"Unknown LLM_PROVIDER: '{provider_name}'. "
        " Valid options: anthropic, openai, gemini"
    )
