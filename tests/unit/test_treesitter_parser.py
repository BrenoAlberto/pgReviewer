from pathlib import Path

from pgreviewer.parsing.treesitter import LANGUAGES, TSParser

_FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "python_sources"
_QUERY_FILE = LANGUAGES[".py"].query_dir / "query_calls.scm"


def _python_source(name: str) -> str:
    return (_FIXTURES_DIR / name).read_text()


def test_parse_file_returns_tree() -> None:
    parser = TSParser("python")

    tree = parser.parse_file("x = 1\n")

    assert tree.root_node.type == "module"


def test_run_query_finds_execute_call_from_fixture() -> None:
    parser = TSParser("python")
    source = _python_source("simple_execute.py")
    query_calls = _QUERY_FILE.read_text()

    tree = parser.parse_file(source)
    matches = parser.run_query(tree, query_calls)

    assert any(
        m["capture"] == "method_name" and m["text"] == "execute" for m in matches
    )
    assert any(
        m["capture"] == "sql_text" and "SELECT id, name, email FROM users" in m["text"]
        for m in matches
    )


def test_languages_registry_contains_python_extension() -> None:
    assert ".py" in LANGUAGES
    assert LANGUAGES[".py"].language_name == "python"
    assert LANGUAGES[".py"].query_dir.name == "python"
