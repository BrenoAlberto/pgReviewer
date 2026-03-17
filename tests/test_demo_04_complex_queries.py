from pathlib import Path

from pgreviewer.config import load_pgreviewer_config

REPO_ROOT = Path(__file__).resolve().parent.parent
DEMO_ROOT = REPO_ROOT / "demos" / "04-complex-queries"


def test_demo_04_config_and_query_contract() -> None:
    config = load_pgreviewer_config(DEMO_ROOT / ".pgreviewer.yml")

    assert config.rules["sequential_scan"].enabled is True
    assert config.rules["sequential_scan"].severity == "critical"
    assert config.rules["sort_without_index"].enabled is True
    assert config.rules["sort_without_index"].severity == "warning"
    assert config.rules["high_cost"].enabled is True
    assert config.rules["high_cost"].severity == "warning"

    reporting_cte = (DEMO_ROOT / "queries" / "reporting_cte.sql").read_text(
        encoding="utf-8"
    )
    window_query = (DEMO_ROOT / "queries" / "window_functions.sql").read_text(
        encoding="utf-8"
    )
    subquery = (DEMO_ROOT / "queries" / "subquery_filter.sql").read_text(
        encoding="utf-8"
    )

    assert "WITH recent_orders AS" in reporting_cte
    assert "JOIN order_items AS oi ON oi.order_id = ro.id" in reporting_cte
    assert "PARTITION BY e.customer_id" in window_query
    assert "ROW_NUMBER() OVER" in window_query
    assert "WHERE (" in subquery
    assert "WHERE o.customer_id = c.id" in subquery


def test_demo_04_seed_and_readme_contract() -> None:
    seed = (DEMO_ROOT / "schema" / "seed.sql").read_text(encoding="utf-8")
    readme = (DEMO_ROOT / "README.md").read_text(encoding="utf-8")

    assert "generate_series(1, 50000)" in seed
    assert "generate_series(1, 100000)" in seed
    assert "ANALYZE customers;" in seed
    assert "ANALYZE event_logs;" in seed

    assert "docker compose up -d db" in readme
    assert "sequential_scan" in readme
    assert "sort_without_index" in readme
    assert "high_cost" in readme
