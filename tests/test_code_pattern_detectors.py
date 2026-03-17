from __future__ import annotations

import importlib
import sys
from pathlib import Path

from pgreviewer.analysis.code_pattern_detectors import (
    CodePatternDetectorRegistry,
    ParsedFile,
    QueryCatalog,
    run_code_pattern_detectors,
)
from pgreviewer.core.models import Issue, Severity
from pgreviewer.parsing.treesitter import TSParser


def test_code_pattern_registry_autodiscovers_detector_module():
    package_dir = (
        Path(__file__).resolve().parents[1]
        / "pgreviewer"
        / "analysis"
        / "code_pattern_detectors"
    )
    module_stem = "_tmp_dynamic_code_pattern_detector"
    module_path = package_dir / f"{module_stem}.py"
    module_name = f"pgreviewer.analysis.code_pattern_detectors.{module_stem}"
    detector_name = "tmp_dynamic_code_pattern_detector"

    module_path.write_text(
        (
            "from pgreviewer.analysis.code_pattern_detectors.base import "
            "ParsedFile, QueryCatalog\n"
            "from pgreviewer.core.models import Issue\n\n"
            "class TmpDynamicCodePatternDetector:\n"
            f"    name = '{detector_name}'\n\n"
            "    def detect(self, files: list[ParsedFile], query_catalog: QueryCatalog)"
            " -> list[Issue]:\n"
            "        return []\n"
        ),
        encoding="utf-8",
    )
    importlib.invalidate_caches()
    sys.modules.pop(module_name, None)

    try:
        detector_names = {
            detector.name for detector in CodePatternDetectorRegistry().all()
        }
        assert detector_name in detector_names
    finally:
        module_path.unlink(missing_ok=True)
        sys.modules.pop(module_name, None)
        importlib.invalidate_caches()


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
