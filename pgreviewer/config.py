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

    # Local Storage Paths
    DEBUG_STORE_PATH: Path = Field(
        Path("~/.pgreviewer/debug").expanduser(),
        description="Directory where raw query plans and debug info are cached",
    )
    COST_STORE_PATH: Path = Field(
        Path("~/.pgreviewer/costs").expanduser(),
        description="Directory where historical cost analysis is stored",
    )


settings = Settings()
