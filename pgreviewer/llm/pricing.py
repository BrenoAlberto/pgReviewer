"""Per-model LLM pricing table.

All costs are in USD per token.  Prices are approximate and should be
updated when providers change their rate cards.

Usage
-----
>>> from pgreviewer.llm.pricing import cost_for_call, estimate_cost
>>> cost_for_call("gpt-4o", input_tokens=500, output_tokens=200)
0.00325
>>> estimate_cost("gemini-2.0-flash", estimated_tokens=1000)
4e-07
"""

from __future__ import annotations

# (input_cost_per_token, output_cost_per_token)
_PRICING: dict[str, tuple[float, float]] = {
    # Anthropic
    "claude-sonnet-4-5": (3.00e-6, 15.00e-6),
    "claude-3-5-haiku-20241022": (0.80e-6, 4.00e-6),
    "claude-opus-4-5": (15.0e-6, 75.00e-6),
    # OpenAI
    "gpt-4o": (2.50e-6, 10.00e-6),
    "gpt-4o-mini": (0.15e-6, 0.60e-6),
    "o1": (15.0e-6, 60.00e-6),
    "o3-mini": (1.10e-6, 4.40e-6),
    # Google Gemini
    "gemini-2.0-flash": (0.10e-6, 0.40e-6),
    "gemini-1.5-pro": (1.25e-6, 5.00e-6),
    "gemini-1.5-flash": (0.075e-6, 0.30e-6),
}

# Used when a model is not in the table
_FALLBACK: tuple[float, float] = (1e-5, 1e-5)


def cost_for_call(model: str, input_tokens: int, output_tokens: int) -> float:
    """Compute exact cost for a completed call."""
    inp, out = _PRICING.get(model, _FALLBACK)
    return inp * input_tokens + out * output_tokens


def estimate_cost(model: str, estimated_tokens: int) -> float:
    """Conservative pre-call estimate.

    Uses the output rate (typically 3-5x higher than input) for the full
    token budget so the guardrail errs on the side of caution.
    """
    _, out = _PRICING.get(model, _FALLBACK)
    return out * estimated_tokens
