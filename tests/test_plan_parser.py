import json
from pathlib import Path

import pytest

from pgreviewer.analysis.plan_parser import parse_explain, walk_nodes
from pgreviewer.core.models import ExplainPlan, PlanNode

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "explain"


@pytest.mark.parametrize(
    "fixture_name", ["seq_scan.json", "index_scan.json", "nested_loop.json"]
)
def test_parse_explain_fixtures(fixture_name):
    fixture_path = FIXTURE_DIR / fixture_name
    with open(fixture_path) as f:
        raw = json.load(f)

    # Postgres EXPLAIN (FORMAT JSON) returns a list
    plan = parse_explain(raw[0])

    assert isinstance(plan, ExplainPlan)
    assert isinstance(plan.root, PlanNode)
    assert plan.root.node_type is not None
    assert plan.root.total_cost >= 0
    assert plan.root.plan_rows >= 0


def test_walk_nodes():
    fixture_path = FIXTURE_DIR / "nested_loop.json"
    with open(fixture_path) as f:
        raw = json.load(f)[0]

    plan = parse_explain(raw)
    nodes = list(walk_nodes(plan))

    assert len(nodes) > 1
    # Check that we visited children
    node_types = [n.node_type for n in nodes]
    assert any("Scan" in nt for nt in node_types)


def test_plan_node_fields():
    # Test specific fields if they exist in the fixtures
    fixture_path = FIXTURE_DIR / "index_scan.json"
    with open(fixture_path) as f:
        raw = json.load(f)[0]

    plan = parse_explain(raw)

    # We expect an Index Scan or at least some Scan with an index if it's not a
    # small table
    # Sometimes Postgres does Seq Scan even with WHERE id=1 if table is tiny,
    # but since it's seeded it should be an index scan.
    # Let's find the scan node
    scan_node = next(n for n in walk_nodes(plan) if "Scan" in n.node_type)
    assert scan_node.node_type is not None
    # If it's an Index Scan, it should have an index name
    if "Index" in scan_node.node_type:
        assert scan_node.index_name is not None
