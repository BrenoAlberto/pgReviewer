import importlib
import pkgutil
from abc import ABC, abstractmethod

from pgreviewer.core.models import ExplainPlan, Issue, SchemaInfo


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
    plan: ExplainPlan, schema: SchemaInfo, disabled_detectors: list[str] | None = None
) -> list[Issue]:
    """
    Helper function to execute all enabled detectors against a plan.
    """
    registry = DetectorRegistry(disabled_detectors=disabled_detectors)
    all_issues = []
    for detector in registry.all():
        issues = detector.detect(plan, schema)
        all_issues.extend(issues)
    return all_issues
