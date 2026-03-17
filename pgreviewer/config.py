from pathlib import Path

from pydantic import Field, PostgresDsn, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
        "http://localhost:8000/mcp",
        description="MCP server URL used for model-context integrations",
    )
    MCP_TIMEOUT_SECONDS: int = Field(
        30,
        description=(
            "Timeout in seconds for MCP connection and initialization operations"
        ),
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
    POSTGRES_VERSION: int = Field(
        11,
        description=(
            "Target PostgreSQL major version used for version-aware migration checks"
        ),
    )


settings = Settings()
