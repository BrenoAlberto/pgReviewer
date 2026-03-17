from dataclasses import dataclass, field
from typing import Any

from pgreviewer.core.models import IndexRecommendation, Issue


@dataclass
class AnalysisResult:
    """Encapsulates the results of a query analysis, including LLM status."""

    issues: list[Issue] = field(default_factory=list)
    recommendations: list[IndexRecommendation] = field(default_factory=list)
    llm_used: bool = False
    llm_degraded: bool = False
    degradation_reason: str | None = None
    raw_explain: Any | None = None
    llm_interpretation: dict[str, Any] | None = None
