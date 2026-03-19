"""LLMProvider protocol — the contract every provider adapter must satisfy."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class LLMProvider(Protocol):
    @property
    def model_name(self) -> str:
        """Canonical model identifier, e.g. ``claude-sonnet-4-5``."""
        ...

    def generate(
        self,
        prompt: str,
        *,
        max_tokens: int,
        temperature: float = 0,
    ) -> tuple[str, int, int]:
        """Call the LLM and return ``(response_text, input_tokens, output_tokens)``.

        Raises
        ------
        LLMUnavailableError
            On any SDK-level error (rate limit, auth failure, network).
            Provider-specific exceptions must not leak past this method.
        """
        ...
