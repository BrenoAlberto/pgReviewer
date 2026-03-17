from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict

import tree_sitter_python as tspython
from tree_sitter import Language, Parser, Query, QueryCursor, Tree

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class LanguageConfig:
    language_name: str
    grammar_loader: Callable[[], Any]
    query_dir: Path


class QueryMatch(TypedDict):
    capture: str
    node: Any
    text: str


_QUERIES_ROOT = Path(__file__).parent / "queries"

LANGUAGES: dict[str, LanguageConfig] = {
    ".py": LanguageConfig(
        language_name="python",
        grammar_loader=tspython.language,
        query_dir=_QUERIES_ROOT / "python",
    )
}


class TSParser:
    def __init__(self, default_language: str = "python") -> None:
        self.default_language = default_language
        self._last_language = default_language
        self._loaded_languages: dict[str, Language] = {}
        self._parsers: dict[str, Parser] = {}

    def _resolve_language(self, language: str) -> LanguageConfig:
        if language in LANGUAGES:
            return LANGUAGES[language]

        for config in LANGUAGES.values():
            if config.language_name == language:
                return config

        raise ValueError(f"Unsupported language: {language}")

    def _get_language(self, language: str) -> Language:
        config = self._resolve_language(language)
        if config.language_name not in self._loaded_languages:
            grammar = config.grammar_loader()
            self._loaded_languages[config.language_name] = Language(grammar)
        return self._loaded_languages[config.language_name]

    def _get_parser(self, language: str) -> Parser:
        config = self._resolve_language(language)
        if config.language_name not in self._parsers:
            self._parsers[config.language_name] = Parser(self._get_language(language))
        return self._parsers[config.language_name]

    def parse_file(self, content: str, language: str | None = None) -> Tree:
        language = language or self.default_language
        self._last_language = self._resolve_language(language).language_name
        parser = self._get_parser(language)
        return parser.parse(content.encode("utf-8"))

    def run_query(self, tree: Tree, query_str: str) -> list[QueryMatch]:
        language = self._get_language(self._last_language)
        query = Query(language, query_str)
        cursor = QueryCursor(query)
        captures = cursor.captures(tree.root_node)

        matches: list[QueryMatch] = []
        for capture_name, nodes in captures.items():
            for node in nodes:
                matches.append(
                    {
                        "capture": capture_name,
                        "node": node,
                        "text": node.text.decode("utf-8"),
                    }
                )

        matches.sort(key=lambda item: item["node"].start_byte)
        return matches
