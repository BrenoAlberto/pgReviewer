from __future__ import annotations

import importlib
import pkgutil
import sys
import types

from pgreviewer.analysis.code_pattern_detectors import (
    CodePatternDetectorRegistry,
    ParsedFile,
    QueryCatalog,
    run_code_pattern_detectors,
)
from pgreviewer.core.models import Issue, Severity
from pgreviewer.parsing.treesitter import TSParser


def test_code_pattern_registry_autodiscovers_detector_module(monkeypatch):
    dynamic_detector_file_stem = "test_dynamic_code_pattern_detector"
    module_name = (
        f"pgreviewer.analysis.code_pattern_detectors.{dynamic_detector_file_stem}"
    )
    detector_name = "test_dynamic_code_pattern_detector"
    module = types.ModuleType(module_name)
    module.__dict__["__name__"] = module_name

    exec(
        (
            "from pgreviewer.analysis.code_pattern_detectors.base import "
            "ParsedFile, QueryCatalog\n"
            "from pgreviewer.core.models import Issue\n\n"
            "class TestDynamicCodePatternDetector:\n"
            f"    name = '{detector_name}'\n\n"
            "    def detect(self, files: list[ParsedFile], query_catalog: QueryCatalog)"
            " -> list[Issue]:\n"
            "        return []\n"
        ),
        module.__dict__,
    )
    original_import_module = importlib.import_module
    original_walk_packages = pkgutil.walk_packages

    def _import_module(name: str, package: str | None = None):
        if name == module_name:
            sys.modules[module_name] = module
            return module
        return original_import_module(name, package=package)

    def _walk_packages(path, prefix):
        if prefix == "pgreviewer.analysis.code_pattern_detectors.":
            return iter([(None, module_name, False)])
        return original_walk_packages(path, prefix)

    monkeypatch.setattr(importlib, "import_module", _import_module)
    monkeypatch.setattr(pkgutil, "walk_packages", _walk_packages)
    sys.modules.pop(module_name, None)

    try:
        detector_names = {
            detector.name for detector in CodePatternDetectorRegistry().all()
        }
        assert detector_name in detector_names
    finally:
        sys.modules.pop(module_name, None)


def test_run_code_pattern_detectors_aggregates_issues(monkeypatch):
    tree = TSParser().parse_file("print('hello')", language="python")
    files = [
        ParsedFile(
            path="app/example.py",
            tree=tree,
            language="python",
            content="print('hello')",
        )
    ]

    class DetectorA:
        name = "detector_a"

        def detect(
            self, files: list[ParsedFile], query_catalog: QueryCatalog
        ) -> list[Issue]:
            return [
                Issue(
                    severity=Severity.INFO,
                    detector_name=self.name,
                    description="A",
                    affected_table=None,
                    affected_columns=[],
                    suggested_action="",
                )
            ]

    class DetectorB:
        name = "detector_b"

        def detect(
            self, files: list[ParsedFile], query_catalog: QueryCatalog
        ) -> list[Issue]:
            return [
                Issue(
                    severity=Severity.WARNING,
                    detector_name=self.name,
                    description="B",
                    affected_table=None,
                    affected_columns=[],
                    suggested_action="",
                )
            ]

    monkeypatch.setattr(
        CodePatternDetectorRegistry,
        "all",
        lambda self: [DetectorA(), DetectorB()],
    )

    issues = run_code_pattern_detectors(files, QueryCatalog())
    assert [issue.detector_name for issue in issues] == ["detector_a", "detector_b"]
