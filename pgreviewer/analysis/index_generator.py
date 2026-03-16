"""SQL generation for index recommendations."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pgreviewer.core.models import IndexRecommendation


def generate_create_index(rec: IndexRecommendation) -> str:
    """Generate a ready-to-run CREATE INDEX CONCURRENTLY statement.

    Parameters
    ----------
    rec:
        The validated index recommendation.

    Returns
    -------
    str
        SQL statement with CONCURRENTLY hint and cost reduction comment.
    """
    # 1. Generate index name: idx_{table}_{col1}_{col2}
    # Max length 63 chars (Postgres limit)
    cols_slug = "_".join(rec.columns)
    base_name = f"idx_{rec.table}_{cols_slug}"

    # Simple sanitization (lowercase plus alphanumeric + underscores)
    index_name = re.sub(r"[^a-z0-9_]", "", base_name.lower())

    if len(index_name) > 63:
        # Truncate and ensure it ends cleanly or use a hash if too long?
        # Requirement says "max 63 chars". We'll truncate.
        index_name = index_name[:63]

    # 2. Build the statement
    unique_clause = "UNIQUE " if rec.is_unique else ""
    idx_type_clause = f"USING {rec.index_type} " if rec.index_type != "btree" else ""
    cols_clause = ", ".join(rec.columns)

    sql = (
        f"CREATE {unique_clause}INDEX CONCURRENTLY {index_name}\n"
        f"ON {rec.table} {idx_type_clause}({cols_clause})"
    )

    if rec.partial_predicate:
        sql += f"\nWHERE {rec.partial_predicate}"

    # 3. Add cost reduction comment
    pc = rec.improvement_pct * 100
    comment = (
        f"-- Estimated cost reduction: {rec.cost_before:.2f} "
        f"→ {rec.cost_after:.2f} ({pc:.1f}%)"
    )

    return f"{comment}\n{sql};"
