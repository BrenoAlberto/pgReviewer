from __future__ import annotations

import importlib
import inspect
import logging
import pkgutil
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from tree_sitter import Tree

    from pgreviewer.core.models import ExtractedQuery, Issue

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ParsedFile:
    path: str
    tree: Tree
    language: str
    content: str


@dataclass(frozen=True)
class QueryCatalog:
    queries: list[ExtractedQuery] = field(default_factory=list)


@runtime_checkable
class CodePatternDetector(Protocol):
    name: str

    def detect(
        self, files: list[ParsedFile], query_catalog: QueryCatalog
    ) -> list[Issue]: ...


class CodePatternDetectorRegistry:
    def __init__(self, disabled_detectors: list[str] | None = None):
        self.disabled_detectors = set(disabled_detectors or [])

    def all(self) -> list[CodePatternDetector]:
        self._load_all_submodules()
        detectors: list[CodePatternDetector] = []
        seen_names: set[str] = set()
        package_prefix = f"{__package__}."

        for module_name, module in list(sys.modules.items()):
            if not module_name.startswith(package_prefix) or module is None:
                continue
            for _, cls in inspect.getmembers(module, inspect.isclass):
                if cls.__module__ != module_name:
                    continue
                if inspect.isabstract(cls):
                    continue

                try:
                    detector = cls()
                except TypeError as exc:
                    logger.debug(
                        "Skipping code pattern detector %s due to constructor "
                        "error: %s",
                        cls.__qualname__,
                        exc,
                    )
                    continue

                if not isinstance(detector, CodePatternDetector):
                    continue

                if (
                    detector.name in self.disabled_detectors
                    or detector.name in seen_names
                ):
                    continue

                seen_names.add(detector.name)
                detectors.append(detector)

        detectors.sort(key=lambda detector: detector.name)
        return detectors

    def _load_all_submodules(self) -> None:
        package_name = __package__
        package = importlib.import_module(package_name)
        for _, module_name, _ in pkgutil.walk_packages(
            package.__path__, package_name + "."
        ):
            importlib.import_module(module_name)


def run_code_pattern_detectors(
    files: list[ParsedFile],
    query_catalog: QueryCatalog,
    disabled_detectors: list[str] | None = None,
) -> list[Issue]:
    registry = CodePatternDetectorRegistry(disabled_detectors=disabled_detectors)
    all_issues: list[Issue] = []
    for detector in registry.all():
        all_issues.extend(detector.detect(files, query_catalog))
    return all_issues
