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
    ) -> None:
        self._cost_store_path = cost_store_path
        self._monthly_budget_usd = monthly_budget_usd
        self._category_limits = category_limits

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def pre_check(self, category: str, estimated_cost_usd: float) -> None:
        """Raise ``BudgetExceededError`` if *estimated_cost_usd* would exceed
        the monthly budget for *category*."""
        limit = self._category_budget(category)
        spent = self._current_spend(category)

        if spent + estimated_cost_usd > limit:
            raise BudgetExceededError(
                f"Budget exceeded for '{category}': "
                f"spent ${spent:.4f} + estimated ${estimated_cost_usd:.4f} "
                f"> limit ${limit:.4f}"
            )

    def record(self, category: str, actual_cost_usd: float) -> None:
        """Append *actual_cost_usd* to the cost log and increment call count."""
        data = self._load()
        cat_data = data.get(category, {"spent": 0.0, "calls": 0})
        cat_data["spent"] += actual_cost_usd
        cat_data["calls"] += 1
        data[category] = cat_data
        self._save(data)

    def reset(self, month: str | None = None) -> None:
        """Clear all spend data for the given *month* (YYYY-MM)."""
        path = self._cost_file(month)
        if path.exists():
            path.unlink()

    def month_summary(self, month: str | None = None) -> list[dict[str, object]]:
        """Return a per-category summary for the specified *month* (YYYY-MM)."""
        data = self._load(month)
        rows: list[dict[str, object]] = []
        for cat, fraction in self._category_limits.items():
            limit = self._monthly_budget_usd * fraction
            cat_data = data.get(cat, {"spent": 0.0, "calls": 0})
            spent = cat_data["spent"]
            calls = cat_data["calls"]
            pct = (spent / limit * 100) if limit > 0 else 0.0
            rows.append(
                {
                    "category": cat,
                    "spent": spent,
                    "limit": limit,
                    "pct": pct,
                    "calls": calls,
                }
            )
        # include categories present in data but not in configured limits
        for cat, cat_data in data.items():
            if cat not in self._category_limits:
                spent = cat_data["spent"]
                calls = cat_data["calls"]
                rows.append(
                    {
                        "category": cat,
                        "spent": spent,
                        "limit": 0.0,
                        "pct": 100.0 if spent > 0 else 0.0,
                        "calls": calls,
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
        cat_data = data.get(category, {"spent": 0.0, "calls": 0})
        return float(cat_data["spent"])

    def _cost_file(self, month: str | None = None) -> Path:
        if month:
            name = f"{month}.json"
        else:
            now = datetime.now(tz=UTC)
            name = f"{now.strftime('%Y-%m')}.json"
        return self._cost_store_path / name

    def _load(self, month: str | None = None) -> dict[str, dict[str, float | int]]:
        path = self._cost_file(month)
        if not path.exists():
            return {}
        with open(path) as f:
            raw_data = json.load(f)

        # Backward compatibility for old float-based logs
        data: dict[str, dict[str, float | int]] = {}
        for cat, val in raw_data.items():
            if isinstance(val, int | float):
                data[cat] = {"spent": float(val), "calls": 0}
            else:
                data[cat] = val
        return data

    def _save(self, data: dict[str, dict[str, float | int]]) -> None:
        path = self._cost_file()
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
