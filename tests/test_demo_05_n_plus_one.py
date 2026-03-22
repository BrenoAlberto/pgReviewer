from pathlib import Path

from pgreviewer.analysis.code_pattern_detectors.base import ParsedFile, QueryCatalog
from pgreviewer.analysis.code_pattern_detectors.python.query_in_loop import (
    QueryInLoopDetector,
)
from pgreviewer.config import load_pgreviewer_config
from pgreviewer.parsing.treesitter import TSParser

REPO_ROOT = Path(__file__).resolve().parent.parent
DEMO_ROOT = REPO_ROOT / "demos" / "05-n-plus-one"
_PARSER = TSParser("python")


def _parse_python(path: Path) -> ParsedFile:
    source = path.read_text(encoding="utf-8")
    return ParsedFile(
        path=str(path.relative_to(DEMO_ROOT)),
        tree=_PARSER.parse_file(source, language="python"),
        language="python",
        content=source,
    )


def test_demo_05_query_in_loop_contract() -> None:
    detector = QueryInLoopDetector()
    parsed_files = [
        _parse_python(DEMO_ROOT / "views.py"),
        _parse_python(DEMO_ROOT / "repository.py"),
    ]

    issues = detector.detect(parsed_files, QueryCatalog())

    assert len(issues) == 2
    assert all(issue.detector_name == "query_in_loop" for issue in issues)
    assert {issue.context["file"] for issue in issues} == {"views.py", "repository.py"}


def test_demo_05_readme_and_config_contract() -> None:
    config = load_pgreviewer_config(DEMO_ROOT / ".pgreviewer.yml")
    readme = (DEMO_ROOT / "README.md").read_text(encoding="utf-8")

    assert config.rules["query_in_loop"].enabled is True

    assert "pgr diff" in readme
    assert "query_in_loop" in readme
    assert "No database" in readme
