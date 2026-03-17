from __future__ import annotations

import ast
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pgreviewer.parsing.treesitter import LANGUAGES, TSParser

_QUERY_FILE = LANGUAGES[".py"].query_dir / "query_calls.scm"
_CACHE_RELATIVE_PATH = Path(".pgreviewer/query_catalog.json")
_SKIP_DIRS = frozenset({".git", "__pycache__", ".venv"})


@dataclass(frozen=True)
class QueryFunctionInfo:
    file: str
    line: int
    method_name: str
    query_text_if_available: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "file": self.file,
            "line": self.line,
            "method_name": self.method_name,
            "query_text_if_available": self.query_text_if_available,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> QueryFunctionInfo:
        return cls(
            file=data["file"],
            line=data["line"],
            method_name=data["method_name"],
            query_text_if_available=data.get("query_text_if_available"),
        )


@dataclass(frozen=True)
class QueryCatalog:
    functions: dict[str, QueryFunctionInfo] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "functions": {
                name: info.to_dict() for name, info in sorted(self.functions.items())
            }
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> QueryCatalog:
        raw_functions = data.get("functions", {})
        functions = {
            name: QueryFunctionInfo.from_dict(info)
            for name, info in raw_functions.items()
            if isinstance(info, dict)
        }
        return cls(functions=functions)

    @property
    def function_names(self) -> set[str]:
        return {fqn.split(".")[-1] for fqn in self.functions}

    def find_by_function_name(self, name: str) -> dict[str, QueryFunctionInfo]:
        return {
            fqn: info
            for fqn, info in self.functions.items()
            if fqn.endswith(f".{name}")
        }


def _iter_python_files(project_root: Path) -> list[Path]:
    return [
        path
        for path in project_root.rglob("*.py")
        if not any(part in _SKIP_DIRS for part in path.parts)
    ]


def _cache_file(project_root: Path) -> Path:
    return project_root / _CACHE_RELATIVE_PATH


def _is_cache_stale(cache_file: Path, files: list[Path]) -> bool:
    if not cache_file.exists():
        return True

    cache_mtime = cache_file.stat().st_mtime
    return any(path.stat().st_mtime > cache_mtime for path in files)


def _load_catalog(cache_file: Path) -> QueryCatalog:
    return QueryCatalog.from_dict(json.loads(cache_file.read_text(encoding="utf-8")))


def _save_catalog(catalog: QueryCatalog, cache_file: Path) -> None:
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps(catalog.to_dict(), indent=2), encoding="utf-8")


def _string_literal_text(node) -> str | None:
    if node.type not in {"string", "concatenated_string"}:
        return None
    if any(child.type == "interpolation" for child in node.children):
        return None
    try:
        value = ast.literal_eval(node.text.decode("utf-8"))
    except (SyntaxError, ValueError):
        return None
    return value if isinstance(value, str) else None


def _query_text_from_call(call_node) -> str | None:
    args_node = call_node.child_by_field_name("arguments")
    if args_node is None or not args_node.named_children:
        return None
    first_arg = args_node.named_children[0]
    if first_arg.type == "keyword_argument":
        first_arg = first_arg.child_by_field_name("value")
        if first_arg is None:
            return None
    return _string_literal_text(first_arg)


def _definition_name(node) -> str | None:
    name_node = node.child_by_field_name("name")
    if name_node is None:
        return None
    return name_node.text.decode("utf-8")


def _enclosing_function_node(node):
    current = node
    while current is not None:
        if current.type == "function_definition":
            return current
        current = current.parent
    return None


def _enclosing_class_node(node):
    current = node
    while current is not None:
        if current.type == "class_definition":
            return current
        current = current.parent
    return None


def _enclosing_call_node(node):
    current = node
    while current is not None:
        if current.type == "call":
            return current
        current = current.parent
    return None


def _module_name(project_root: Path, file_path: Path) -> str:
    relative = file_path.relative_to(project_root).with_suffix("")
    return ".".join(relative.parts)


def _query_method_name(call_node) -> str | None:
    function = call_node.child_by_field_name("function")
    if function is None:
        return None
    if function.type == "attribute":
        method = function.child_by_field_name("attribute")
        return method.text.decode("utf-8") if method is not None else None
    if function.type == "identifier":
        return function.text.decode("utf-8")
    return None


def _build_catalog(project_root: Path, files: list[Path]) -> QueryCatalog:
    parser = TSParser("python")
    query_source = _QUERY_FILE.read_text(encoding="utf-8")
    functions: dict[str, QueryFunctionInfo] = {}

    for file_path in files:
        try:
            content = file_path.read_text(encoding="utf-8")
            tree = parser.parse_file(content, language="python")
        except (OSError, UnicodeDecodeError):
            continue

        matches = parser.run_query(tree, query_source)
        seen_calls: set[tuple[int, int]] = set()
        method_nodes = [
            match["node"] for match in matches if match["capture"] == "method_name"
        ]
        for method_node in method_nodes:
            call_node = _enclosing_call_node(method_node)
            if call_node is None:
                continue
            call_key = (call_node.start_byte, call_node.end_byte)
            if call_key in seen_calls:
                continue
            seen_calls.add(call_key)

            function_node = _enclosing_function_node(call_node)
            if function_node is None:
                continue

            function_name = _definition_name(function_node)
            if function_name is None:
                continue

            class_name = None
            class_node = _enclosing_class_node(function_node)
            if class_node is not None:
                class_name = _definition_name(class_node)

            module = _module_name(project_root, file_path)
            parts = [module]
            if class_name:
                parts.append(class_name)
            parts.append(function_name)
            fqn = ".".join(parts)
            query_method_name = _query_method_name(call_node)
            if query_method_name is None:
                continue

            info = QueryFunctionInfo(
                file=file_path.relative_to(project_root).as_posix(),
                line=function_node.start_point[0] + 1,
                method_name=query_method_name,
                query_text_if_available=_query_text_from_call(call_node),
            )

            existing = functions.get(fqn)
            if existing is None or (
                existing.query_text_if_available is None
                and info.query_text_if_available is not None
            ):
                functions[fqn] = info

    return QueryCatalog(functions=functions)


def build_catalog(project_root: Path, force_rebuild: bool = False) -> QueryCatalog:
    project_root = project_root.resolve()
    files = _iter_python_files(project_root)
    cache_file = _cache_file(project_root)

    if not force_rebuild and not _is_cache_stale(cache_file, files):
        try:
            return _load_catalog(cache_file)
        except (OSError, json.JSONDecodeError, KeyError, TypeError):
            pass

    catalog = _build_catalog(project_root, files)
    _save_catalog(catalog, cache_file)
    return catalog
