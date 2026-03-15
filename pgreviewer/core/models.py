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


class IssueSeverity(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    WARNING = "WARNING"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class Issue(BaseModel):
    detector_name: str
    severity: IssueSeverity
    message: str
    context: dict[str, Any] = Field(default_factory=dict)


class SchemaInfo(BaseModel):
    # Placeholder for table and index metadata
    tables: dict[str, dict[str, Any]] = Field(default_factory=dict)
    indexes: dict[str, dict[str, Any]] = Field(default_factory=dict)
