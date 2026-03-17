from fnmatch import fnmatch
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    PostgresDsn,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from pgreviewer.core.models import Issue, Severity
from pgreviewer.exceptions import ConfigError


class RuleConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    severity: Literal["info", "warning", "critical"] | None = None


class ThresholdConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seq_scan_rows: int | None = None
    high_cost: float | None = None
    hypopg_min_improvement: float | None = None
    large_table_ddl_rows: int | None = None


class IgnoreConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tables: list[str] = Field(default_factory=list)
    files: list[str] = Field(default_factory=list)
    rules: list[str] = Field(default_factory=list)


class PgReviewerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rules: dict[str, RuleConfig] = Field(default_factory=dict)
    thresholds: ThresholdConfig = Field(default_factory=ThresholdConfig)
    ignore: IgnoreConfig = Field(default_factory=IgnoreConfig)


def _format_validation_error(path: Path, exc: ValidationError) -> str:
    lines: list[str] = [f"Invalid configuration in {path}:"]
    for error in exc.errors():
        location = " -> ".join(str(part) for part in error["loc"])
        lines.append(f"- {location}: {error['msg']}")
    return "\n".join(lines)


def load_pgreviewer_config(path: Path = Path(".pgreviewer.yml")) -> PgReviewerConfig:
    if not path.exists():
        return PgReviewerConfig()
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigError(f"Failed to read {path}: {exc}") from exc

    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ConfigError(f"Invalid configuration in {path}: root must be a mapping")
    try:
        return PgReviewerConfig.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(_format_validation_error(path, exc)) from exc


def _disabled_rules(config: PgReviewerConfig) -> set[str]:
    disabled = set(config.ignore.rules)
    disabled.update(name for name, rule in config.rules.items() if not rule.enabled)
    return disabled


def _settings_overrides(config: PgReviewerConfig) -> dict[str, object]:
    overrides: dict[str, object] = {
        "IGNORE_TABLES": config.ignore.tables,
        "IGNORE_PATHS": config.ignore.files,
    }

    if config.thresholds.seq_scan_rows is not None:
        overrides["SEQ_SCAN_ROW_THRESHOLD"] = config.thresholds.seq_scan_rows
    if config.thresholds.high_cost is not None:
        overrides["HIGH_COST_THRESHOLD"] = config.thresholds.high_cost
    if config.thresholds.hypopg_min_improvement is not None:
        overrides["HYPOPG_MIN_IMPROVEMENT"] = config.thresholds.hypopg_min_improvement
    if config.thresholds.large_table_ddl_rows is not None:
        overrides["LARGE_TABLE_DDL_THRESHOLD"] = config.thresholds.large_table_ddl_rows
    return overrides


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database Configuration
    DATABASE_URL: PostgresDsn = Field(
        ...,
        description="PostgreSQL connection string (e.g., postgresql://user:pass@host:5432/db)",
    )

    # Operational Modes
    READ_ONLY: bool = Field(
        True,
        description="If True, the tool will focus on analysis and avoid writing to DB",
    )

    # AI/LLM Configuration
    LLM_API_KEY: str | None = Field(
        None,
        description="Optional API key for LLM-powered review insights",
    )
    MCP_SERVER_URL: str = Field(
        "http://localhost:8000/sse",
        description="MCP server SSE endpoint URL (e.g. http://localhost:8000/sse)",
    )
    MCP_TIMEOUT_SECONDS: int = Field(
        30,
        description=(
            "Timeout in seconds for MCP connection and initialization operations"
        ),
    )
    BACKEND: str = Field(
        "local",
        description="Analysis backend selector: local, mcp, or hybrid",
    )

    # Detection Thresholds
    SEQ_SCAN_ROW_THRESHOLD: int = Field(
        10_000,
        description="Min rows in a table before a seq scan is flagged",
    )
    SEQ_SCAN_CRITICAL_THRESHOLD: int = Field(
        1_000_000,
        description="Min rows for a seq scan to be considered CRITICAL",
    )
    NESTED_LOOP_OUTER_THRESHOLD: int = Field(
        1_000,
        description="Min outer-relation rows before a nested loop join is flagged",
    )
    CONCURRENT_INDEX_THRESHOLD: int = Field(
        100_000,
        description="Min rows before non-concurrent index is CRITICAL",
    )
    TABLE_REWRITE_THRESHOLD: int = Field(
        50_000,
        description="Min rows before a table rewrite (ALTER TYPE) is CRITICAL",
    )
    LARGE_TABLE_DDL_THRESHOLD: int = Field(
        10_000_000,
        description="Min rows before any DDL on a table triggers a WARNING",
    )
    HIGH_COST_THRESHOLD: float = Field(
        10_000.0,
        description="Queries exceeding this plan cost will be flagged",
    )
    HIGH_COST_CRITICAL_THRESHOLD: float = Field(
        100_000.0,
        description="Queries exceeding this plan cost will be flagged as CRITICAL",
    )
    HYPOPG_MIN_IMPROVEMENT: float = Field(
        0.30,
        description="Min relative cost improvement required to recommend an index",
    )

    # LLM Budget Configuration
    LLM_MONTHLY_BUDGET_USD: float = Field(
        10.0,
        description="Total monthly budget for LLM calls in USD",
    )
    LLM_BUDGET_INTERPRETATION: float = Field(
        0.45,
        description="Fraction of monthly budget for EXPLAIN plan analysis",
    )
    LLM_BUDGET_EXTRACTION: float = Field(
        0.30,
        description="Fraction of monthly budget for SQL extraction from code",
    )
    LLM_BUDGET_REPORTING: float = Field(
        0.20,
        description="Fraction of monthly budget for generating review reports",
    )
    LLM_BUDGET_CLASSIFICATION: float = Field(
        0.05,
        description="Fraction of monthly budget for LLM-based code classification",
    )

    @model_validator(mode="after")
    def validate_llm_fractions(self) -> "Settings":
        total = (
            self.LLM_BUDGET_INTERPRETATION
            + self.LLM_BUDGET_EXTRACTION
            + self.LLM_BUDGET_REPORTING
            + self.LLM_BUDGET_CLASSIFICATION
        )
        if abs(total - 1.0) > 0.001:
            from pgreviewer.exceptions import ConfigError

            raise ConfigError(
                f"LLM budget fractions must sum to 1.0, current sum: {total}"
            )
        return self

    @property
    def llm_category_limits(self) -> dict[str, float]:
        """Maps category names to their configured fraction of the budget."""
        return {
            "interpretation": self.LLM_BUDGET_INTERPRETATION,
            "extraction": self.LLM_BUDGET_EXTRACTION,
            "reporting": self.LLM_BUDGET_REPORTING,
            "classification": self.LLM_BUDGET_CLASSIFICATION,
        }

    """
    this is intentionally a placeholder. Since no LLM client exists yet
    (
        per the issue:
        "this builds the infrastructure so it is already in place when "
        "Story 3.1 wires up the client"
    ),
    adding model-specific pricing now would be premature.
    When Story 3.1 wires up the actual LLM integration, we can replace this
    single default with a per-model lookup (e.g. a dict mapping model names
    to input/output token rates).

    For now, pre_check just needs some cost-per-token to do the budget math.
    """
    LLM_COST_PER_TOKEN: float = Field(
        0.00001,
        description="Estimated cost per token in USD for budget pre-checks",
    )

    # Local Storage Paths
    DEBUG_STORE_PATH: Path = Field(
        Path("~/.pgreviewer/debug").expanduser(),
        description="Directory where raw query plans and debug info are cached",
    )
    COST_STORE_PATH: Path = Field(
        Path("~/.pgreviewer/costs").expanduser(),
        description="Directory where historical cost analysis is stored",
    )

    DISABLED_DETECTORS: list[str] = Field(
        default_factory=list,
        description="List of detector names to skip during analysis",
    )

    QUERY_METHODS: list[str] = Field(
        default_factory=lambda: [
            "execute",
            "fetch",
            "fetchrow",
            "fetchval",
            "fetchone",
            "fetchall",
        ],
        description=(
            "Known DB query method names used by code-pattern detectors "
            "(e.g. execute, fetch, fetchrow)."
        ),
    )
    QUERY_IN_LOOP_IGNORE_PATTERNS: list[str] = Field(
        default_factory=list,
        description=(
            "Path glob patterns used only by query_in_loop detector "
            "(e.g. ['*/management/commands/*'])."
        ),
    )
    QUERY_IN_LOOP_FUNCTION_ALLOWLIST: list[str] = Field(
        default_factory=list,
        description=(
            "Function/method names considered batch-safe for query_in_loop "
            "(e.g. ['bulk_create', 'executemany'])."
        ),
    )

    IGNORE_TABLES: list[str] = Field(
        default_factory=list,
        description="List of table names to exclude from all issue detectors",
    )

    IGNORE_PATHS: list[str] = Field(
        default_factory=list,
        description=(
            "List of glob patterns for file paths to skip during classification "
            "(e.g. ['docs/*', 'tests/fixtures/**'])"
        ),
    )
    TRIGGER_PATHS: Annotated[list[str], NoDecode] = Field(
        default_factory=list,
        validation_alias=AliasChoices("TRIGGER_PATHS", "INPUT_TRIGGER_PATHS"),
        description=(
            "Optional comma-separated glob patterns that limit diff analysis to "
            "matching files (e.g. 'migrations/**,app/models.py')."
        ),
    )
    POSTGRES_VERSION: int = Field(
        11,
        description=(
            "Target PostgreSQL major version used for version-aware migration checks"
        ),
    )

    @field_validator("TRIGGER_PATHS", mode="before")
    @classmethod
    def _parse_trigger_paths(cls, value: object) -> object:
        if isinstance(value, str):
            return [part.strip() for part in value.split(",") if part.strip()]
        return value


_project_config_error: ConfigError | None = None
try:
    _project_config = load_pgreviewer_config()
except ConfigError as exc:
    _project_config = PgReviewerConfig()
    _project_config_error = exc

settings = Settings(**_settings_overrides(_project_config))
settings.DISABLED_DETECTORS = sorted(
    set(settings.DISABLED_DETECTORS) | _disabled_rules(_project_config)
)


def ensure_project_config_is_valid() -> None:
    if _project_config_error is not None:
        raise _project_config_error


def apply_issue_config(issues: list[Issue]) -> list[Issue]:
    ensure_project_config_is_valid()
    suppressed = set(settings.DISABLED_DETECTORS) | _disabled_rules(_project_config)
    configured_rules = _project_config.rules
    filtered: list[Issue] = []

    for issue in issues:
        if issue.detector_name in suppressed:
            continue

        if issue.affected_table and any(
            fnmatch(issue.affected_table.lower(), pattern.lower())
            for pattern in settings.IGNORE_TABLES
        ):
            continue

        rule = configured_rules.get(issue.detector_name)
        if rule and rule.severity is not None:
            issue.severity = Severity[rule.severity.upper()]
        filtered.append(issue)

    return filtered
