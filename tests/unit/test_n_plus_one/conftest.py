from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from pgreviewer.analysis.code_pattern_detectors.base import ParsedFile
from pgreviewer.parsing.treesitter import TSParser

_PARSER = TSParser("python")
FIXTURES_DIR = Path(__file__).parents[2] / "fixtures" / "n_plus_one"


def parse_python_path(path: Path) -> ParsedFile:
    source = path.read_text(encoding="utf-8")
    return ParsedFile(
        path=str(path),
        tree=_PARSER.parse_file(source, language="python"),
        language="python",
        content=source,
    )


def parse_python_source(path: str, source: str) -> ParsedFile:
    return ParsedFile(
        path=path,
        tree=_PARSER.parse_file(source, language="python"),
        language="python",
        content=source,
    )


@pytest.fixture
def fixture_project(tmp_path: Path) -> Path:
    destination = tmp_path / "n_plus_one"
    shutil.copytree(FIXTURES_DIR, destination)
    return destination
