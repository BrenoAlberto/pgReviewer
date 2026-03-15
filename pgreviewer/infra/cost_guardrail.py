from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pgreviewer.exceptions import BudgetExceededError

if TYPE_CHECKING:
    from pathlib import Path


class CostGuardrail:
    """Guard against runaway LLM spend by enforcing per-category monthly budgets."""

    def __init__(
        self,
        cost_store_path: Path,
        monthly_budget_usd: float,
        category_limits: dict[str, float],
        cost_per_token: float,
    ) -> None:
        self._cost_store_path = cost_store_path
        self._monthly_budget_usd = monthly_budget_usd
        self._category_limits = category_limits
        self._cost_per_token = cost_per_token

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def pre_check(self, category: str, estimated_tokens: int) -> None:
        """Raise ``BudgetExceededError`` if *estimated_tokens* would blow the
        monthly budget for *category*."""
        estimated_cost = estimated_tokens * self._cost_per_token
        limit = self._category_budget(category)
        spent = self._current_spend(category)

        if spent + estimated_cost > limit:
            raise BudgetExceededError(
                f"Budget exceeded for '{category}': "
                f"spent ${spent:.4f} + estimated ${estimated_cost:.4f} "
                f"> limit ${limit:.4f}"
            )

    def record(self, category: str, actual_cost_usd: float) -> None:
        """Append *actual_cost_usd* to the cost log for *category*."""
        data = self._load()
        data[category] = data.get(category, 0.0) + actual_cost_usd
        self._save(data)

    def month_summary(self) -> list[dict[str, object]]:
        """Return a per-category summary for the current month."""
        data = self._load()
        rows: list[dict[str, object]] = []
        for cat, fraction in self._category_limits.items():
            limit = self._monthly_budget_usd * fraction
            spent = data.get(cat, 0.0)
            pct = (spent / limit * 100) if limit > 0 else 0.0
            rows.append(
                {
                    "category": cat,
                    "spent": spent,
                    "limit": limit,
                    "pct": pct,
                }
            )
        # include categories present in data but not in configured limits
        for cat in data:
            if cat not in self._category_limits:
                spent = data[cat]
                rows.append(
                    {
                        "category": cat,
                        "spent": spent,
                        "limit": 0.0,
                        "pct": 100.0,
                    }
                )
        return rows

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _category_budget(self, category: str) -> float:
        fraction = self._category_limits.get(category, 0.0)
        return self._monthly_budget_usd * fraction

    def _current_spend(self, category: str) -> float:
        data = self._load()
        return data.get(category, 0.0)

    def _cost_file(self) -> Path:
        now = datetime.now(tz=UTC)
        return self._cost_store_path / f"{now.strftime('%Y-%m')}.json"

    def _load(self) -> dict[str, float]:
        path = self._cost_file()
        if not path.exists():
            return {}
        with open(path) as f:
            return json.load(f)

    def _save(self, data: dict[str, float]) -> None:
        path = self._cost_file()
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
