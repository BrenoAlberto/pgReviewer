from dataclasses import dataclass, field
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
    is_unique: bool = False  # Added for compatibility
    partial_predicate: str | None = None
    create_statement: str = ""  # ready-to-run SQL
    cost_before: float = 0.0
    cost_after: float = 0.0
    improvement_pct: float = 0.0
    estimated_size_bytes: int | None = None
    validated: bool = False  # True = HypoPG confirmed improvement
    rationale: str = ""  # human-readable explanation
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "table": self.table,
            "columns": self.columns,
            "index_type": self.index_type,
            "is_unique": self.is_unique,
            "partial_predicate": self.partial_predicate,
            "create_statement": self.create_statement,
            "cost_before": self.cost_before,
            "cost_after": self.cost_after,
            "improvement_pct": self.improvement_pct,
            "estimated_size_bytes": self.estimated_size_bytes,
            "validated": self.validated,
            "rationale": self.rationale,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "IndexRecommendation":
        return cls(**data)


@dataclass
class ExtractedQuery:
    sql: str
    line_number: int
    file_path: str
    extraction_method: str = "treesitter"
    confidence: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)
