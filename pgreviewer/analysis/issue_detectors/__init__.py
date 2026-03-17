import importlib
import logging
import pkgutil
from abc import ABC, abstractmethod

from pgreviewer.config import PgReviewerConfig, Settings, apply_issue_config
from pgreviewer.core.models import ExplainPlan, Issue, SchemaInfo
from pgreviewer.parsing.suppression_parser import parse_inline_suppressions

logger = logging.getLogger(__name__)


class BaseDetector(ABC):
    """
    Abstract base class for all query plan issue detectors.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """The machine-readable name of the detector."""
        pass

    @abstractmethod
    def detect(self, plan: ExplainPlan, schema: SchemaInfo) -> list[Issue]:
        """
        Analyze a plan and schema to find potential issues.
        """
        pass


class DetectorRegistry:
    """
    Registry for discovering and instantiating detectors.
    """

    def __init__(self, disabled_detectors: list[str] | None = None):
        self.disabled_detectors = disabled_detectors or []

    def all(self) -> list[BaseDetector]:
        """
        Discovers all BaseDetector subclasses in this package and returns
        instances of the enabled ones.
        """
        # Ensure all modules in the current package are loaded to trigger
        # subclass registration
        self._load_all_submodules()

        detectors = []
        for cls in BaseDetector.__subclasses__():
            # Check if this class is concrete
            if not getattr(cls, "__abstractmethods__", None):
                detector = cls()
                if detector.name not in self.disabled_detectors:
                    detectors.append(detector)
        return detectors

    def migration_detectors(self) -> list:
        from pgreviewer.analysis.migration_detectors import BaseMigrationDetector

        self._load_all_migration_submodules()
        detectors = []
        for cls in BaseMigrationDetector.__subclasses__():
            if not getattr(cls, "__abstractmethods__", None):
                detector = cls()
                if detector.name not in self.disabled_detectors:
                    detectors.append(detector)
        return detectors

    def _load_all_submodules(self) -> None:
        """
        Iterate through all modules in the issue_detectors package and import them.
        """
        package_name = __name__
        package = importlib.import_module(package_name)
        for _, module_name, _ in pkgutil.walk_packages(
            package.__path__, package_name + "."
        ):
            importlib.import_module(module_name)

    def _load_all_migration_submodules(self) -> None:
        package_name = "pgreviewer.analysis.migration_detectors"
        package = importlib.import_module(package_name)
        for _, module_name, _ in pkgutil.walk_packages(
            package.__path__, package_name + "."
        ):
            importlib.import_module(module_name)


def run_all_detectors(
    plan: ExplainPlan,
    schema: SchemaInfo,
    disabled_detectors: list[str] | None = None,
    project_config: PgReviewerConfig | None = None,
    runtime_settings: Settings | None = None,
    source_sql: str | None = None,
    suppression_stats: dict[str, int] | None = None,
) -> list[Issue]:
    """
    Helper function to execute all enabled detectors against a plan.
    """
    registry = DetectorRegistry(disabled_detectors=disabled_detectors)
    all_issues = []
    detectors = registry.all()
    known_rules = {detector.name for detector in detectors}
    for detector in detectors:
        issues = detector.detect(plan, schema)
        all_issues.extend(issues)

    suppression = parse_inline_suppressions(source_sql or "", known_rules=known_rules)
    for unknown_rule in suppression.unknown_rules:
        logger.warning(
            "Unknown rule '%s' in pgreviewer:ignore comment",
            unknown_rule,
        )

    suppressed_count = 0
    if suppression.suppress_all or suppression.rules:
        filtered_issues: list[Issue] = []
        for issue in all_issues:
            if suppression.suppresses(issue.detector_name):
                suppressed_count += 1
                logger.debug(
                    "Suppressed issue '%s' via pgreviewer:ignore comment",
                    issue.detector_name,
                )
                continue
            filtered_issues.append(issue)
        all_issues = filtered_issues

    if suppression_stats is not None:
        suppression_stats["suppressed_issues"] = suppressed_count

    return apply_issue_config(
        all_issues,
        project=project_config,
        runtime_settings=runtime_settings,
    )
