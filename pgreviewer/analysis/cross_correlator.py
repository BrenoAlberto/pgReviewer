from __future__ import annotations

import re
from dataclasses import dataclass

from pgreviewer.core.models import Issue, Severity

_ADD_COLUMN_RE = re.compile(
    r"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?P<table>[^\s(]+)\s+ADD\s+COLUMN\s+(?P<column>[^\s(]+)",
    re.IGNORECASE,
)
_DROP_INDEX_RE = re.compile(
    r"DROP\s+INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+EXISTS\s+)?(?P<index>[^\s(;,]+)",
    re.IGNORECASE,
)


@dataclass
class CrossCuttingFinding:
    issue: Issue
    migration_file: str
    migration_line: int
    query_file: str
    query_line: int


def correlate_findings(results: list[dict]) -> list[CrossCuttingFinding]:
    migration_events: list[dict] = []
    query_entries: list[dict] = []

    for entry in results:
        query_obj = entry["query_obj"]
        sql = query_obj.sql
        source = {
            "file": query_obj.source_file,
            "line": query_obj.line_number,
            "entry": entry,
            "sql": sql,
        }

        if query_obj.extraction_method == "migration_sql":
            add_match = _ADD_COLUMN_RE.search(sql)
            if add_match:
                migration_events.append(
                    {
                        "type": "add_column",
                        "table": _norm(add_match.group("table")),
                        "column": _norm(add_match.group("column")),
                        **source,
                    }
                )

            drop_match = _DROP_INDEX_RE.search(sql)
            if drop_match:
                index_name = _norm(drop_match.group("index"))
                migration_events.append(
                    {
                        "type": "drop_index",
                        "index_name": index_name,
                        "index_columns": _infer_index_columns(index_name),
                        **source,
                    }
                )
        else:
            query_entries.append(source)

    findings: list[CrossCuttingFinding] = []

    for migration_entry in migration_events:
        if migration_entry["type"] == "add_column":
            findings.extend(_correlate_added_column(migration_entry, query_entries))
            continue
        findings.extend(_correlate_dropped_index(migration_entry, query_entries))

    findings.extend(_correlate_fk_without_index(results, query_entries))
    return findings


def _correlate_added_column(
    migration_entry: dict,
    query_entries: list[dict],
) -> list[CrossCuttingFinding]:
    findings: list[CrossCuttingFinding] = []
    table = migration_entry["table"]
    column = migration_entry["column"]

    for query_entry in query_entries:
        matched_issue = next(
            (
                issue
                for issue in query_entry["entry"]["issues"]
                if issue.detector_name == "missing_index_on_filter"
                and issue.affected_table == table
                and column in issue.affected_columns
            ),
            None,
        )
        if not matched_issue:
            continue

        _remove_issue(query_entry["entry"], matched_issue)
        findings.append(
            CrossCuttingFinding(
                issue=Issue(
                    severity=Severity.CRITICAL,
                    detector_name="cross_cutting_add_column_query_without_index",
                    description=(
                        f"Migration adds column '{table}.{column}' and query in same "
                        "diff filters on it without an index."
                    ),
                    affected_table=table,
                    affected_columns=[column],
                    suggested_action=(
                        f"Create an index for {table}({column}) before merging this "
                        "migration and query together."
                    ),
                    cause_file=migration_entry["file"],
                    cause_line=migration_entry["line"],
                    cause_context=f"column `{table}.{column}` added here",
                ),
                migration_file=migration_entry["file"],
                migration_line=migration_entry["line"],
                query_file=query_entry["file"],
                query_line=query_entry["line"],
            )
        )
    return findings


def _correlate_dropped_index(
    migration_entry: dict,
    query_entries: list[dict],
) -> list[CrossCuttingFinding]:
    findings: list[CrossCuttingFinding] = []
    index_columns = set(migration_entry["index_columns"])
    if not index_columns:
        return findings

    for query_entry in query_entries:
        for issue in list(query_entry["entry"]["issues"]):
            if not set(issue.affected_columns).intersection(index_columns):
                continue
            if issue.detector_name not in {
                "missing_index_on_filter",
                "sort_without_index",
                "sequential_scan",
            }:
                continue

            _remove_issue(query_entry["entry"], issue)
            findings.append(
                CrossCuttingFinding(
                    issue=Issue(
                        severity=Severity.CRITICAL,
                        detector_name="cross_cutting_drop_index_query_usage",
                        description=(
                            f"Migration drops index '{migration_entry['index_name']}' "
                            "and query in same diff uses its indexed columns."
                        ),
                        affected_table=issue.affected_table,
                        affected_columns=sorted(index_columns),
                        suggested_action=(
                            "Keep the index, replace it with an equivalent one, or "
                            "change the query to avoid regressing performance."
                        ),
                        cause_file=migration_entry["file"],
                        cause_line=migration_entry["line"],
                        cause_context=(
                            f"index `{migration_entry['index_name']}` dropped here"
                        ),
                    ),
                    migration_file=migration_entry["file"],
                    migration_line=migration_entry["line"],
                    query_file=query_entry["file"],
                    query_line=query_entry["line"],
                )
            )
            break
    return findings


def _correlate_fk_without_index(
    results: list[dict],
    query_entries: list[dict],
) -> list[CrossCuttingFinding]:
    findings: list[CrossCuttingFinding] = []

    for entry in results:
        query_obj = entry["query_obj"]
        if query_obj.extraction_method != "migration_sql":
            continue

        for issue in list(entry["issues"]):
            if issue.detector_name != "add_foreign_key_without_index":
                continue

            table = issue.affected_table
            columns = set(issue.affected_columns)
            matched_query = next(
                (
                    q
                    for q in query_entries
                    if _is_join_query(q["sql"])
                    and (not table or table in q["sql"].lower())
                    and _contains_any_column(q["sql"], columns)
                ),
                None,
            )
            if not matched_query:
                continue

            _remove_issue(entry, issue)
            findings.append(
                CrossCuttingFinding(
                    issue=Issue(
                        severity=Severity.CRITICAL,
                        detector_name="cross_cutting_fk_without_index_join",
                        description=(
                            f"Migration adds FK on '{table}' without index and query "
                            "in same diff joins on that FK column."
                        ),
                        affected_table=table,
                        affected_columns=sorted(columns),
                        suggested_action=issue.suggested_action,
                        cause_file=query_obj.source_file,
                        cause_line=query_obj.line_number,
                        cause_context=(f"FK on `{table}` without index added here"),
                    ),
                    migration_file=query_obj.source_file,
                    migration_line=query_obj.line_number,
                    query_file=matched_query["file"],
                    query_line=matched_query["line"],
                )
            )
    return findings


def _remove_issue(entry: dict, issue_to_remove: Issue) -> None:
    entry["issues"] = [i for i in entry["issues"] if i is not issue_to_remove]


def _norm(identifier: str) -> str:
    return identifier.strip().strip('"').lower()


def _infer_index_columns(index_name: str) -> list[str]:
    tokens = [t for t in _norm(index_name).split("_") if t]
    if not tokens:
        return []
    if tokens[0] in {"idx", "index"}:
        tokens = tokens[1:]
    if len(tokens) > 1:
        tokens = tokens[1:]
    return tokens


def _is_join_query(sql: str) -> bool:
    return " join " in f" {sql.lower()} "


def _contains_any_column(sql: str, columns: set[str]) -> bool:
    lowered = sql.lower()
    return any(re.search(rf"\b{re.escape(col.lower())}\b", lowered) for col in columns)
