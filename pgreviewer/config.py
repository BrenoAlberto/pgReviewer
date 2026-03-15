from pathlib import Path

from pydantic import Field, PostgresDsn
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

    # Detection Thresholds
    SEQ_SCAN_ROW_THRESHOLD: int = Field(
        10_000,
        description="Min rows in a table before a seq scan is flagged",
    )
    HIGH_COST_THRESHOLD: float = Field(
        10_000.0,
        description="Queries exceeding this plan cost will be flagged",
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
    LLM_CATEGORY_LIMITS: dict[str, float] = Field(
        default={"review": 0.5, "summary": 0.3, "general": 0.2},
        description="Per-category budget split as fractions of total monthly budget",
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

    IGNORE_TABLES: list[str] = Field(
        default_factory=list,
        description="List of table names to exclude from all issue detectors",
    )


settings = Settings()
