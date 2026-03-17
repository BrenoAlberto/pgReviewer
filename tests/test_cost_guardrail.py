from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from pgreviewer.exceptions import BudgetExceededError
from pgreviewer.infra.cost_guardrail import CostGuardrail


@pytest.fixture()
def cost_dir(tmp_path: Path) -> Path:
    return tmp_path / "costs"


def _make_guardrail(
    cost_dir: Path,
    monthly_budget: float = 10.0,
    category_limits: dict[str, float] | None = None,
    cost_per_token: float = 0.00001,
) -> CostGuardrail:
    if category_limits is None:
        category_limits = {"review": 0.5, "summary": 0.3, "general": 0.2}
    return CostGuardrail(
        cost_store_path=cost_dir,
        monthly_budget_usd=monthly_budget,
        category_limits=category_limits,
        cost_per_token=cost_per_token,
    )


# ── pre_check: under-budget passes ──────────────────────────────────


class TestPreCheckUnderBudget:
    def test_under_budget_does_not_raise(self, cost_dir: Path) -> None:
        g = _make_guardrail(cost_dir)
        # 10 tokens * 0.00001 = $0.0001, well under $5.00 limit for review (50%)
        g.pre_check("review", estimated_tokens=10)

    def test_zero_tokens_passes(self, cost_dir: Path) -> None:
        g = _make_guardrail(cost_dir)
        g.pre_check("review", estimated_tokens=0)


# ── pre_check: at-limit blocks ──────────────────────────────────────


class TestPreCheckAtLimit:
    def test_exceeding_budget_raises(self, cost_dir: Path) -> None:
        g = _make_guardrail(cost_dir, monthly_budget=1.0, cost_per_token=0.01)
        # review limit = 1.0 * 0.5 = $0.50
        # record $0.49 spent already
        g.record("review", 0.49)
        # 2 tokens * $0.01 = $0.02 → 0.49 + 0.02 = 0.51 > 0.50
        with pytest.raises(BudgetExceededError):
            g.pre_check("review", estimated_tokens=2)

    def test_exactly_at_limit_passes(self, cost_dir: Path) -> None:
        g = _make_guardrail(cost_dir, monthly_budget=1.0, cost_per_token=0.01)
        # review limit = $0.50; record $0.49
        g.record("review", 0.49)
        # 1 token * $0.01 = $0.01 → 0.49 + 0.01 = 0.50 == limit → passes
        g.pre_check("review", estimated_tokens=1)

    def test_unknown_category_has_zero_limit(self, cost_dir: Path) -> None:
        g = _make_guardrail(cost_dir, monthly_budget=1.0, cost_per_token=0.01)
        with pytest.raises(BudgetExceededError):
            g.pre_check("nonexistent", estimated_tokens=1)


# ── record persists costs ───────────────────────────────────────────


class TestRecord:
    def test_record_creates_file(self, cost_dir: Path) -> None:
        g = _make_guardrail(cost_dir)
        g.record("review", 0.05)
        files = list(cost_dir.glob("*.json"))
        assert len(files) == 1
        data = json.loads(files[0].read_text())
        assert data["review"]["spent"] == pytest.approx(0.05)
        assert data["review"]["calls"] == 1

    def test_record_accumulates(self, cost_dir: Path) -> None:
        g = _make_guardrail(cost_dir)
        g.record("review", 0.05)
        g.record("review", 0.03)
        files = list(cost_dir.glob("*.json"))
        data = json.loads(files[0].read_text())
        assert data["review"]["spent"] == pytest.approx(0.08)
        assert data["review"]["calls"] == 2

    def test_record_multiple_categories(self, cost_dir: Path) -> None:
        g = _make_guardrail(cost_dir)
        g.record("review", 0.10)
        g.record("summary", 0.20)
        files = list(cost_dir.glob("*.json"))
        data = json.loads(files[0].read_text())
        assert data["review"]["spent"] == pytest.approx(0.10)
        assert data["summary"]["spent"] == pytest.approx(0.20)
        assert data["review"]["calls"] == 1
        assert data["summary"]["calls"] == 1

    # ── new month resets counter ────────────────────────────────────────

    def test_different_month_file_means_zero_spend(self, cost_dir: Path) -> None:
        """A cost file from a prior month does not affect the current month."""
        g = _make_guardrail(cost_dir, monthly_budget=1.0, cost_per_token=0.01)
        # Manually write an old month file
        cost_dir.mkdir(parents=True, exist_ok=True)
        old_file = cost_dir / "1999-01.json"
        old_file.write_text(json.dumps({"review": {"spent": 999.0, "calls": 1}}))
        # Current month has no file → spend is 0 → pre_check passes
        g.pre_check("review", estimated_tokens=1)


# ── month_summary ───────────────────────────────────────────────────


class TestMonthSummary:
    def test_summary_all_zeros(self, cost_dir: Path) -> None:
        g = _make_guardrail(cost_dir, monthly_budget=10.0)
        rows = g.month_summary()
        assert len(rows) == 3  # review, summary, general
        for r in rows:
            assert r["spent"] == 0.0
            assert r["pct"] == 0.0
            assert r["calls"] == 0

    def test_summary_reflects_spend(self, cost_dir: Path) -> None:
        g = _make_guardrail(cost_dir, monthly_budget=10.0)
        g.record("review", 2.50)
        rows = g.month_summary()
        review_row = next(r for r in rows if r["category"] == "review")
        assert review_row["spent"] == pytest.approx(2.50)
        assert review_row["limit"] == pytest.approx(5.0)  # 10 * 0.5
        assert review_row["pct"] == pytest.approx(50.0)
        assert review_row["calls"] == 1

    def test_summary_specific_month(self, cost_dir: Path) -> None:
        g = _make_guardrail(cost_dir, monthly_budget=10.0)
        # Write directly to skip dynamic filename logic
        cost_dir.mkdir(parents=True, exist_ok=True)
        (cost_dir / "2024-01.json").write_text(
            json.dumps({"review": {"spent": 1.0, "calls": 5}})
        )

        rows = g.month_summary(month="2024-01")
        review_row = next(r for r in rows if r["category"] == "review")
        assert review_row["spent"] == 1.0
        assert review_row["calls"] == 5

    def test_summary_backward_compatibility(self, cost_dir: Path) -> None:
        g = _make_guardrail(cost_dir, monthly_budget=10.0)
        cost_dir.mkdir(parents=True, exist_ok=True)
        (cost_dir / "2023-01.json").write_text(json.dumps({"review": 2.0}))

        rows = g.month_summary(month="2023-01")
        review_row = next(r for r in rows if r["category"] == "review")
        assert review_row["spent"] == 2.0
        assert review_row["calls"] == 0


class TestReset:
    def test_reset_deletes_file(self, cost_dir: Path) -> None:
        g = _make_guardrail(cost_dir)
        g.record("review", 0.05)
        path = list(cost_dir.glob("*.json"))[0]
        assert path.exists()

        g.reset()
        assert not path.exists()

    def test_reset_specific_month(self, cost_dir: Path) -> None:
        g = _make_guardrail(cost_dir)
        cost_dir.mkdir(parents=True, exist_ok=True)
        old_file = cost_dir / "2024-01.json"
        old_file.write_text("{}")
        assert old_file.exists()

        g.reset(month="2024-01")
        assert not old_file.exists()
