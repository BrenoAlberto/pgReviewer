from __future__ import annotations

import re
from functools import lru_cache
from typing import TYPE_CHECKING

from pgreviewer.core.models import Issue, ParsedMigration, Severity, SlowQuery

if TYPE_CHECKING:
    from pgreviewer.parsing.model_differ import ModelDiff

_DROP_INDEX_RE = re.compile(
    r"DROP\s+INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+EXISTS\s+)?(?P<index>[^\s(;,]+)",
    re.IGNORECASE,
)
_WHERE_OR_JOIN_RE = re.compile(r"\b(?:where|join)\b", re.IGNORECASE)
_MAX_INDEX_NAME_COLUMN_PART_WINDOW = 3


def _normalize_identifier(identifier: str) -> str:
    return identifier.strip().strip('"').lower()


def _strip_schema(index_name: str) -> str:
    return _normalize_identifier(index_name).split(".")[-1]


def _infer_columns_from_index_name(index_name: str) -> list[str]:
    tokens = [part for part in _strip_schema(index_name).split("_") if part]
    if not tokens:
        return []
    if tokens[0] in {"idx", "ix", "index"}:
        tokens = tokens[1:]
    if len(tokens) > 1:
        tokens = tokens[1:]
    if not tokens:
        return []

    inferred: set[str] = {"_".join(tokens)}
    if len(tokens) > 1:
        for width in range(2, min(_MAX_INDEX_NAME_COLUMN_PART_WINDOW, len(tokens)) + 1):
            for start in range(0, len(tokens) - width + 1):
                inferred.add("_".join(tokens[start : start + width]))
    return sorted(name for name in inferred if name)


@lru_cache(maxsize=512)
def _column_match_pattern(column: str) -> re.Pattern[str]:
    return re.compile(rf"(?<![\w$]){re.escape(column)}(?![\w$])")


def _query_uses_any_column(query_text: str, columns: list[str]) -> bool:
    lowered = query_text.lower()
    if _WHERE_OR_JOIN_RE.search(lowered) is None:
        return False
    return any(_column_match_pattern(column).search(lowered) for column in columns)


def _build_issue(
    index_name: str,
    table: str | None,
    columns: list[str],
    matching_queries: list[SlowQuery],
) -> Issue | None:
    if not matching_queries:
        return None
    total_calls = sum(item.calls for item in matching_queries)
    weighted_total_ms = sum(
        item.mean_exec_time_ms * item.calls for item in matching_queries
    )
    avg_ms = weighted_total_ms / total_calls if total_calls else 0.0
    index_usage_label = "this column" if len(columns) == 1 else "these columns"
    return Issue(
        severity=Severity.CRITICAL,
        detector_name="drop_index_workload",
        description=(
            f"WARNING: dropping {index_name} — {len(matching_queries)} queries in "
            f"pg_stat_statements use {index_usage_label} "
            f"({total_calls}/day, avg {avg_ms:.1f}ms)"
        ),
        affected_table=table,
        affected_columns=list(columns),
        suggested_action=(
            "Do not drop this index without a replacement strategy validated against "
            "production workload."
        ),
        context={
            "index_name": index_name,
            "matching_queries": len(matching_queries),
            "total_calls_per_day": total_calls,
            "avg_exec_time_ms": round(avg_ms, 1),
        },
    )


def detect_drop_index_workload_issues(
    migration: ParsedMigration, slow_queries: list[SlowQuery]
) -> list[Issue]:
    if not slow_queries:
        return []

    issues: list[Issue] = []
    for statement in migration.statements:
        match = _DROP_INDEX_RE.search(statement.raw_sql)
        if not match:
            continue
        index_name = _strip_schema(match.group("index"))
        columns = _infer_columns_from_index_name(index_name)
        if not columns:
            continue
        matching_queries = [
            query
            for query in slow_queries
            if _query_uses_any_column(query.query_text, columns)
        ]
        issue = _build_issue(
            index_name=index_name,
            table=statement.table,
            columns=columns,
            matching_queries=matching_queries,
        )
        if issue is not None:
            issues.append(issue)
    return issues


def detect_removed_index_workload_issues(
    diff: ModelDiff, slow_queries: list[SlowQuery]
) -> list[Issue]:
    if not slow_queries:
        return []

    issues: list[Issue] = []
    for idx in diff.removed_indexes:
        index_name = _strip_schema(idx.name or "unnamed_index")
        columns = [_normalize_identifier(col) for col in idx.columns if col]
        if not columns:
            columns = _infer_columns_from_index_name(index_name)
        if not columns:
            continue
        matching_queries = [
            query
            for query in slow_queries
            if _query_uses_any_column(query.query_text, columns)
        ]
        issue = _build_issue(
            index_name=index_name,
            table=diff.table_name,
            columns=columns,
            matching_queries=matching_queries,
        )
        if issue is not None:
            issues.append(issue)
    return issues
