import re

from pgreviewer.analysis.issue_detectors import BaseDetector
from pgreviewer.analysis.plan_parser import walk_nodes
from pgreviewer.config import settings
from pgreviewer.core.models import ExplainPlan, Issue, IssueSeverity, SchemaInfo

# SQL keywords that are never column names
_SQL_KEYWORDS = frozenset(
    {
        "and",
        "or",
        "not",
        "is",
        "null",
        "true",
        "false",
        "in",
        "like",
        "ilike",
        "between",
        "any",
        "all",
        "exists",
        "case",
        "when",
        "then",
        "else",
        "end",
    }
)

# Matches a bare identifier that immediately precedes a comparison operator.
# Supported operators:
#   =, <>, !=, <=, >=, <, >   — standard comparisons
#   ~~  / !~~                  — internal Postgres form of LIKE / NOT LIKE
#   ~~* / !~~*                 — internal Postgres form of ILIKE / NOT ILIKE
#   IS, LIKE, ILIKE, IN, BETWEEN — SQL keyword operators
_COLUMN_RE = re.compile(
    r"\b([a-z_][a-z0-9_]*)\s*"
    r"(?:=|<>|!=|<=|>=|<|>|~~\*?|!~~\*?|(?:IS|LIKE|ILIKE|IN|BETWEEN)\b)",
    re.IGNORECASE,
)


def _extract_filter_columns(filter_expr: str) -> list[str]:
    """Return the column names referenced on the left-hand side of predicates."""
    return [
        m
        for m in _COLUMN_RE.findall(filter_expr)
        if m.lower() not in _SQL_KEYWORDS
    ]


def _has_covering_index(
    table: str, columns: list[str], indexes: dict[str, dict]
) -> bool:
    """Return True if any index on *table* has one of *columns* as its leading column.

    Each entry in *indexes* is expected to have the shape::

        {
            "table": "orders",
            "columns": ["user_id", ...],   # ordered list; first element is leading
        }
    """
    for index_meta in indexes.values():
        if index_meta.get("table") != table:
            continue
        index_columns = index_meta.get("columns", [])
        if not index_columns:
            continue
        # An index covers the predicate when its leading column matches any
        # of the filter columns.
        if index_columns[0] in columns:
            return True
    return False


class MissingIndexOnFilterDetector(BaseDetector):
    """
    Detects Seq Scan nodes that have a Filter condition but no covering index.

    For every ``Seq Scan`` node that carries a ``Filter`` expression the
    detector extracts the referenced column(s), consults ``SchemaInfo.indexes``
    to see whether a suitable index already exists, and emits an :class:`Issue`
    when none is found.

    Tables listed in ``settings.IGNORE_TABLES`` are silently skipped.
    """

    @property
    def name(self) -> str:
        return "missing_index_on_filter"

    def detect(self, plan: ExplainPlan, schema: SchemaInfo) -> list[Issue]:
        issues: list[Issue] = []

        for node in walk_nodes(plan):
            if node.node_type != "Seq Scan":
                continue
            if not node.filter_expr:
                continue

            table_name = node.relation_name or node.alias_name
            if not table_name:
                continue

            if table_name in settings.IGNORE_TABLES:
                continue

            columns = _extract_filter_columns(node.filter_expr)
            if not columns:
                continue

            if _has_covering_index(table_name, columns, schema.indexes):
                continue

            issues.append(
                Issue(
                    detector_name=self.name,
                    severity=IssueSeverity.MEDIUM,
                    message=(
                        f"Seq Scan on '{table_name}' filters on "
                        f"{columns} but no covering index exists"
                    ),
                    context={
                        "affected_table": table_name,
                        "filter_expr": node.filter_expr,
                        "suggested_columns": columns,
                        "suggested_action": (
                            f"Consider adding an index on "
                            f"{table_name}({', '.join(columns)})"
                        ),
                    },
                )
            )

        return issues
