from pgreviewer.analysis.issue_detectors import BaseDetector
from pgreviewer.analysis.plan_parser import walk_nodes
from pgreviewer.core.models import ExplainPlan, Issue, SchemaInfo, Severity


def _has_covering_index(
    table: str, columns: list[str], indexes: dict[str, dict]
) -> bool:
    """
    Return True if any index on *table* has the first sort column as its leading column.

    This is a simplified check matching the requirement: "Only flag when no
    suitable index exists".
    A B-tree index on (A, B) can satisfy ORDER BY A or ORDER BY A, B.
    """
    if not columns:
        return False

    first_col = columns[0]

    for index_meta in indexes.values():
        if index_meta.get("table") != table:
            continue
        index_columns = index_meta.get("columns", [])
        if not index_columns:
            continue

        # If the first column of the sort key is the first column of the index,
        # Postgres can generally use it to satisfy the sort.
        if index_columns[0] == first_col:
            return True
    return False


class SortWithoutIndexDetector(BaseDetector):
    """
    Detects Sort nodes where the operation could be avoided by using an index.
    """

    @property
    def name(self) -> str:
        return "sort_without_index"

    def detect(self, plan: ExplainPlan, schema: SchemaInfo) -> list[Issue]:
        issues: list[Issue] = []

        for node in walk_nodes(plan):
            if node.node_type != "Sort":
                continue

            # Only flag for non-trivial sorts (> 1000 rows input)
            # The input is the child of the Sort node
            if not node.children or node.children[0].plan_rows <= 1000:
                continue

            # Try to find the relation name from the child or sub-children
            table_name = None
            for sub_node in walk_nodes(node):
                if sub_node.relation_name:
                    table_name = sub_node.relation_name
                    break

            if not table_name:
                continue

            # Check if sort key columns are indexed
            if not node.sort_key:
                continue

            # Clean sort keys (Postgres often includes ASC/DESC or alias prefix)
            clean_sort_keys = []
            for key in node.sort_key:
                # Basic cleaning: remove " DESC", " NULLS FIRST", etc if present
                # And remove table prefix if any
                clean_key = key.split()[0].split(".")[-1]
                clean_sort_keys.append(clean_key)

            if _has_covering_index(table_name, clean_sort_keys, schema.indexes):
                continue

            issues.append(
                Issue(
                    detector_name=self.name,
                    severity=Severity.WARNING,
                    description=(
                        f"Explicit Sort on '{table_name}' using columns "
                        f"{clean_sort_keys} on {node.children[0].plan_rows:,} rows. "
                        "Consider an index to allow index scan order."
                    ),
                    affected_table=table_name,
                    affected_columns=clean_sort_keys,
                    suggested_action=(
                        f"Add an index on {table_name}"
                        f"({', '.join(clean_sort_keys)}) "
                        "to allow index scan order"
                    ),
                    context={
                        "input_rows": node.children[0].plan_rows,
                    },
                )
            )

        return issues
