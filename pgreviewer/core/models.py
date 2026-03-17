from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any

from pydantic import AliasChoices, BaseModel, Field


class PlanNode(BaseModel):
    node_type: str = Field(alias=AliasChoices("Node Type", "node_type"))
    total_cost: float = Field(alias=AliasChoices("Total Cost", "total_cost"))
    startup_cost: float = Field(alias=AliasChoices("Startup Cost", "startup_cost"))
    plan_rows: int = Field(alias=AliasChoices("Plan Rows", "plan_rows"))
    plan_width: int = Field(alias=AliasChoices("Plan Width", "plan_width"))

    # Optional fields depending on node type
    filter_expr: str | None = Field(
        default=None, alias=AliasChoices("Filter", "filter_expr")
    )
    index_name: str | None = Field(
        default=None, alias=AliasChoices("Index Name", "index_name")
    )
    index_cond: str | None = Field(
        default=None, alias=AliasChoices("Index Cond", "index_cond")
    )
    join_type: str | None = Field(
        default=None, alias=AliasChoices("Join Type", "join_type")
    )
    relation_name: str | None = Field(
        default=None, alias=AliasChoices("Relation Name", "relation_name")
    )
    alias_name: str | None = Field(
        default=None, alias=AliasChoices("Alias", "alias_name")
    )
    sort_key: list[str] = Field(
        default_factory=list, alias=AliasChoices("Sort Key", "sort_key")
    )
    hash_cond: str | None = Field(
        default=None, alias=AliasChoices("Hash Cond", "hash_cond")
    )
    merge_cond: str | None = Field(
        default=None, alias=AliasChoices("Merge Cond", "merge_cond")
    )
    join_filter: str | None = Field(
        default=None, alias=AliasChoices("Join Filter", "join_filter")
    )

    children: list["PlanNode"] = Field(
        default_factory=list, alias=AliasChoices("Plans", "children")
    )


class ExplainPlan(BaseModel):
    root: PlanNode
    planning_time: float | None = Field(
        default=None, alias=AliasChoices("Planning Time", "planning_time")
    )
    execution_time: float | None = Field(
        default=None, alias=AliasChoices("Execution Time", "execution_time")
    )

    # Allow extra fields for custom metadata


class Severity(StrEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass
class Issue:
    severity: Severity
    detector_name: str
    description: str
    affected_table: str | None
    affected_columns: list[str]
    suggested_action: str
    confidence: float = 1.0
    context: dict[str, Any] = field(default_factory=dict)


class IndexInfo(BaseModel):
    name: str
    columns: list[str]
    is_unique: bool = False
    is_partial: bool = False
    index_type: str = "btree"


class ColumnInfo(BaseModel):
    name: str
    type: str
    null_fraction: float = 0.0
    distinct_count: float = 0.0
    most_common_freqs: list[float] = Field(default_factory=list)


class TableInfo(BaseModel):
    row_estimate: int = 0
    size_bytes: int = 0
    indexes: list[IndexInfo] = Field(default_factory=list)
    columns: list[ColumnInfo] = Field(default_factory=list)


class SchemaInfo(BaseModel):
    tables: dict[str, TableInfo] = Field(default_factory=dict)


@dataclass
class IndexRecommendation:
    table: str
    columns: list[str]
    index_type: str = "btree"  # btree, hash, gin, gist
    is_unique: bool = False
    partial_predicate: str | None = None
    create_statement: str = ""  # ready-to-run SQL
    cost_before: float = 0.0
    cost_after: float = 0.0
    improvement_pct: float = 0.0
    estimated_size_bytes: int | None = None
    validated: bool = False  # True = HypoPG confirmed improvement
    rationale: str = ""  # human-readable explanation
    notes: list[str] = field(default_factory=list)  # additional notes (e.g. redundancy)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "IndexRecommendation":
        return cls(**data)


@dataclass
class ExtractedQuery:
    sql: str
    source_file: str
    line_number: int
    extraction_method: str  # "migration_sql", "alembic_execute", "ast", "llm"
    confidence: float  # 0.0–1.0
    notes: str | None = None  # e.g. "parameterized query, substituted dummy values"

    def to_dict(self) -> dict[str, Any]:
        return {
            "sql": self.sql,
            "source_file": self.source_file,
            "line_number": self.line_number,
            "extraction_method": self.extraction_method,
            "confidence": self.confidence,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExtractedQuery":
        return cls(**data)


@dataclass
class DDLStatement:
    statement_type: str
    table: str | None
    raw_sql: str
    line_number: int


@dataclass
class ParsedMigration:
    statements: list[DDLStatement]
    source_file: str
