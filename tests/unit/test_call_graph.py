from pgreviewer.analysis.call_graph import build_shallow_call_graph, resolve_to_query
from pgreviewer.analysis.code_pattern_detectors.base import ParsedFile
from pgreviewer.analysis.query_catalog import QueryCatalog, QueryFunctionInfo
from pgreviewer.parsing.treesitter import TSParser


def _parsed_python_file(path: str, source: str) -> ParsedFile:
    parser = TSParser("python")
    return ParsedFile(
        path=path,
        tree=parser.parse_file(source, language="python"),
        language="python",
        content=source,
    )


def test_build_shallow_call_graph_records_called_functions() -> None:
    parsed_file = _parsed_python_file(
        "app/service.py",
        "def process_order(order_id):\n"
        "    helper(order_id)\n"
        "    repo.get_order(order_id)\n",
    )

    call_graph = build_shallow_call_graph([parsed_file])

    assert call_graph["process_order"] == {"helper", "get_order"}


def test_resolve_to_query_depth_one() -> None:
    call_graph = {"process_order": {"get_order"}}
    catalog = QueryCatalog(
        functions={
            "repository.OrderRepository.get_order": QueryFunctionInfo(
                file="repository.py",
                line=10,
                method_name="fetchone",
                query_text_if_available="SELECT * FROM orders WHERE id = :id",
            )
        }
    )

    resolved = resolve_to_query("process_order", call_graph, catalog, max_depth=1)

    assert resolved == catalog.functions["repository.OrderRepository.get_order"]


def test_resolve_to_query_depth_two() -> None:
    call_graph = {"process_order": {"normalize"}, "normalize": {"get_order"}}
    catalog = QueryCatalog(
        functions={
            "repository.OrderRepository.get_order": QueryFunctionInfo(
                file="repository.py",
                line=14,
                method_name="fetchone",
                query_text_if_available="SELECT * FROM orders WHERE id = :id",
            )
        }
    )

    resolved = resolve_to_query("process_order", call_graph, catalog, max_depth=2)

    assert resolved == catalog.functions["repository.OrderRepository.get_order"]


def test_resolve_to_query_returns_none_beyond_max_depth() -> None:
    call_graph = {
        "process_order": {"step_one"},
        "step_one": {"step_two"},
        "step_two": {"get_order"},
    }
    catalog = QueryCatalog(
        functions={
            "repository.OrderRepository.get_order": QueryFunctionInfo(
                file="repository.py",
                line=20,
                method_name="fetchone",
                query_text_if_available="SELECT * FROM orders WHERE id = :id",
            )
        }
    )

    resolved = resolve_to_query("process_order", call_graph, catalog, max_depth=2)

    assert resolved is None
