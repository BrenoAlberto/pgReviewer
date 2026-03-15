import json
from pathlib import Path

import pytest

from pgreviewer.analysis.plan_parser import extract_tables, parse_explain, walk_nodes
from pgreviewer.core.models import ExplainPlan, PlanNode

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "explain"


def _load(fixture_name: str) -> ExplainPlan:
    with open(FIXTURE_DIR / fixture_name) as f:
        raw = json.load(f)
    return parse_explain(raw[0])


@pytest.mark.parametrize(
    "fixture_name",
    [
        "seq_scan_large.json",
        "index_scan.json",
        "nested_loop_large_outer.json",
        "cartesian_join.json",
        "sort_without_index.json",
    ],
)
def test_parse_explain_fixtures(fixture_name):
    """All required fixtures must parse into a valid ExplainPlan."""
    plan = _load(fixture_name)

    assert isinstance(plan, ExplainPlan)
    assert isinstance(plan.root, PlanNode)
    assert plan.root.node_type is not None
    assert plan.root.total_cost >= 0
    assert plan.root.plan_rows >= 0


@pytest.mark.parametrize(
    "fixture_name,expected_root_type",
    [
        ("seq_scan_large.json", "Seq Scan"),
        ("index_scan.json", "Index Scan"),
        ("nested_loop_large_outer.json", "Nested Loop"),
        ("cartesian_join.json", "Nested Loop"),
        ("sort_without_index.json", "Sort"),
    ],
)
def test_root_node_type(fixture_name, expected_root_type):
    """Root node type must match the fixture's expected plan shape."""
    plan = _load(fixture_name)
    assert plan.root.node_type == expected_root_type


@pytest.mark.parametrize(
    "fixture_name,min_cost",
    [
        ("seq_scan_large.json", 1_000.0),
        ("nested_loop_large_outer.json", 10_000.0),
        ("cartesian_join.json", 1_000_000.0),
        ("sort_without_index.json", 50_000.0),
    ],
)
def test_root_total_cost(fixture_name, min_cost):
    """Root node total cost must be at or above the expected minimum."""
    plan = _load(fixture_name)
    assert plan.root.total_cost >= min_cost


def test_seq_scan_large_filter_extraction():
    """seq_scan_large.json must expose a non-None filter expression."""
    plan = _load("seq_scan_large.json")
    assert plan.root.filter_expr is not None
    assert "status" in plan.root.filter_expr


def test_index_scan_index_fields():
    """index_scan.json root must expose index_name and index_cond."""
    plan = _load("index_scan.json")
    assert plan.root.index_name is not None
    assert plan.root.index_cond is not None


def test_sort_without_index_sort_key():
    """sort_without_index.json root must expose at least one sort key."""
    plan = _load("sort_without_index.json")
    assert plan.root.sort_key
    assert "created_at" in plan.root.sort_key


def test_nested_loop_large_outer_row_count():
    """nested_loop_large_outer.json outer child must have >= 500 000 rows."""
    plan = _load("nested_loop_large_outer.json")
    outer = plan.root.children[0]
    assert outer.plan_rows >= 500_000


def test_walk_nodes():
    """walk_nodes must visit every node in the tree depth-first."""
    plan = _load("nested_loop_large_outer.json")
    nodes = list(walk_nodes(plan))

    assert len(nodes) > 1
    node_types = [n.node_type for n in nodes]
    assert any("Scan" in nt for nt in node_types)


def test_walk_nodes_cartesian_join():
    """walk_nodes on cartesian_join.json must reach the leaf Seq Scan nodes."""
    plan = _load("cartesian_join.json")
    nodes = list(walk_nodes(plan))
    seq_scans = [n for n in nodes if n.node_type == "Seq Scan"]
    assert len(seq_scans) >= 2


def test_walk_nodes_sort_without_index():
    """walk_nodes on sort_without_index.json must yield Sort then Seq Scan."""
    plan = _load("sort_without_index.json")
    nodes = list(walk_nodes(plan))
    types = [n.node_type for n in nodes]
    assert types[0] == "Sort"
    assert "Seq Scan" in types


def test_extract_tables_index_scan():
    """extract_tables on index_scan.json must return ['users']."""
    plan = _load("index_scan.json")
    assert extract_tables(plan) == ["users"]


def test_extract_tables_nested_loop_large_outer():
    """extract_tables must return both tables from nested_loop_large_outer.json."""
    plan = _load("nested_loop_large_outer.json")
    tables = extract_tables(plan)
    assert "orders" in tables
    assert "users" in tables


def test_extract_tables_sort_without_index():
    """extract_tables on sort_without_index.json must return ['orders']."""
    plan = _load("sort_without_index.json")
    assert extract_tables(plan) == ["orders"]
