"""Model-diff issue detectors.

Analyse :class:`~pgreviewer.parsing.model_differ.ModelDiff` objects (produced
by comparing two versions of a SQLAlchemy model file) and return
:class:`~pgreviewer.core.models.Issue` instances describing performance or
correctness problems that can be inferred from the structural changes alone –
without requiring a live database connection.

Public API
----------
- :func:`detect_missing_fk_index` – CRITICAL when a FK column has no index
- :func:`detect_removed_index`    – WARNING/CRITICAL when a named index is removed
- :func:`detect_large_text_without_constraint` – INFO when unconstrained text added
- :func:`detect_duplicate_pk_index` – WARNING when an explicit index duplicates the PK
- :func:`run_model_issue_detectors` – convenience wrapper that runs all checks
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pgreviewer.config import PgReviewerConfig, Settings, apply_issue_config
from pgreviewer.core.models import Issue, SchemaInfo, Severity

if TYPE_CHECKING:
    from pgreviewer.parsing.model_differ import ModelDiff

# ---------------------------------------------------------------------------
# Individual detectors
# ---------------------------------------------------------------------------

_UNCONSTRAINED_TEXT_TYPES: frozenset[str] = frozenset({"Text", "text", "UnicodeText"})


def detect_missing_fk_index(
    diff: ModelDiff, source_file: str | None = None
) -> list[Issue]:
    """Flag added ``ForeignKey`` columns that have no corresponding index.

    PostgreSQL does **not** automatically create an index on a foreign-key
    column (unlike primary keys).  A missing index on a FK column causes
    sequential scans on every JOIN or lookup against that column.

    An added FK column is considered indexed when **any** of the following
    is true:

    * The column was declared with ``index=True`` or ``unique=True``.
    * A new :class:`~pgreviewer.parsing.sqlalchemy_analyzer.IndexDef` entry
      in ``diff.added_indexes`` references the column.

    Parameters
    ----------
    diff:
        The :class:`~pgreviewer.parsing.model_differ.ModelDiff` to inspect.

    Returns
    -------
    list[Issue]
        One CRITICAL :class:`~pgreviewer.core.models.Issue` per unindexed FK.
    """
    issues: list[Issue] = []

    # Build the set of column names that are covered by an added index.
    # Primary key columns are auto-indexed by PostgreSQL (including composite PKs),
    # so they do not need an explicit index for FK constraint checks.
    indexed_columns: set[str] = set()
    for idx in diff.added_indexes:
        indexed_columns.update(idx.columns)
    for col in diff.added_columns:
        if col.index or col.unique or col.primary_key:
            indexed_columns.add(col.name)

    for fk in diff.added_foreign_keys:
        if fk.column_name in indexed_columns:
            continue
        issues.append(
            Issue(
                detector_name="missing_fk_index",
                severity=Severity.CRITICAL,
                description=(f"Missing index on foreign key column `{fk.column_name}`"),
                affected_table=diff.table_name,
                affected_columns=[fk.column_name],
                suggested_action=(
                    f"Add `index=True` to the `{fk.column_name}` column definition "
                    "or create an explicit `Index(...)` on this column. "
                    "PostgreSQL does not auto-create indexes on foreign key columns."
                ),
                context={
                    "fk_target": fk.target,
                    "class_name": diff.class_name,
                    "line_number": fk.line or None,
                },
                cause_file=source_file,
                cause_line=fk.line or None,
                cause_context=(
                    f"FK column `{fk.column_name}` referencing `{fk.target}` "
                    "added without index"
                ),
            )
        )

    return issues


def detect_removed_index(
    diff: ModelDiff, schema: SchemaInfo, source_file: str | None = None
) -> list[Issue]:
    """Flag removed named indexes that may be relied on by existing queries.

    Severity is CRITICAL when live schema data shows the table has more than
    100,000 rows (high traffic); WARNING otherwise or when schema data is not
    available.

    Parameters
    ----------
    diff:
        The :class:`~pgreviewer.parsing.model_differ.ModelDiff` to inspect.
    schema:
        Live schema information.  Pass an empty :class:`SchemaInfo` when no
        database connection is available.

    Returns
    -------
    list[Issue]
        One issue per removed named index.
    """
    issues: list[Issue] = []

    for idx in diff.removed_indexes:
        cols = ", ".join(idx.columns) if idx.columns else "(unknown)"

        table_info = schema.tables.get(diff.table_name)
        row_estimate = table_info.row_estimate if table_info else 0
        severity = Severity.CRITICAL if row_estimate > 100_000 else Severity.WARNING

        index_label = f"`{idx.name}`" if idx.name else f"on `{cols}`"
        issues.append(
            Issue(
                detector_name="removed_index",
                severity=severity,
                description=(
                    f"Index {index_label} covering column(s) `{cols}` was removed"
                ),
                affected_table=diff.table_name,
                affected_columns=list(idx.columns),
                suggested_action=(
                    "Verify that no existing queries rely on this index. "
                    "Removing an index can cause sequential scans and significant "
                    "performance degradation on large tables."
                ),
                context={
                    "index_name": idx.name,
                    "class_name": diff.class_name,
                    "row_estimate": row_estimate,
                    # removed_index has no line in the new file — idx.line is from
                    # the base branch. Skip line number for removed items.
                },
                cause_file=source_file,
                # idx.line is the base-branch line; omit since it is not a valid
                # new-file line and would confuse diff-line anchoring.
                cause_context=f"index {index_label} on `{cols}` removed here",
            )
        )

    return issues


def detect_large_text_without_constraint(
    diff: ModelDiff, source_file: str | None = None
) -> list[Issue]:
    """Flag newly added ``Text`` or unconstrained ``String`` columns.

    ``Text`` is always unbounded.  A ``String`` column declared without an
    explicit length (e.g. ``String`` instead of ``String(255)``) is also
    unconstrained.  On high-traffic tables this can degrade storage and query
    efficiency.

    Parameters
    ----------
    diff:
        The :class:`~pgreviewer.parsing.model_differ.ModelDiff` to inspect.

    Returns
    -------
    list[Issue]
        One INFO :class:`~pgreviewer.core.models.Issue` per unconstrained column.
    """
    issues: list[Issue] = []

    for col in diff.added_columns:
        is_unconstrained_text = col.col_type in _UNCONSTRAINED_TEXT_TYPES
        is_unconstrained_string = col.col_type == "String" and not col.has_type_args

        if not (is_unconstrained_text or is_unconstrained_string):
            continue

        issues.append(
            Issue(
                detector_name="large_text_without_constraint",
                severity=Severity.INFO,
                description=(
                    f"Column `{col.name}` uses unconstrained type `{col.col_type}`"
                ),
                affected_table=diff.table_name,
                affected_columns=[col.name],
                suggested_action=(
                    "Consider using `String(n)` with an explicit length limit "
                    "to avoid unbounded storage and improve query performance."
                ),
                context={
                    "col_type": col.col_type,
                    "class_name": diff.class_name,
                    "line_number": col.line or None,
                },
                cause_file=source_file,
                cause_line=col.line or None,
                cause_context=(
                    f"unconstrained column `{col.name}` ({col.col_type}) added here"
                ),
            )
        )

    return issues


def detect_duplicate_pk_index(
    diff: ModelDiff, source_file: str | None = None
) -> list[Issue]:
    """Flag explicitly added indexes that duplicate the implicit primary-key index.

    PostgreSQL automatically creates a unique B-tree index for every primary
    key.  Adding a separate ``Index(...)`` on the same column(s) wastes storage
    and adds write overhead without any query-planning benefit.

    Parameters
    ----------
    diff:
        The :class:`~pgreviewer.parsing.model_differ.ModelDiff` to inspect.

    Returns
    -------
    list[Issue]
        One WARNING :class:`~pgreviewer.core.models.Issue` per redundant index.
    """
    issues: list[Issue] = []

    if not diff.pk_columns:
        return issues

    pk_set = set(diff.pk_columns)

    for idx in diff.added_indexes:
        if not idx.columns:
            continue
        if all(col in pk_set for col in idx.columns):
            cols = ", ".join(idx.columns)
            index_label = f"`{idx.name}`" if idx.name else f"on `{cols}`"
            issues.append(
                Issue(
                    detector_name="duplicate_pk_index",
                    severity=Severity.WARNING,
                    description=(
                        f"Index {index_label} duplicates the implicit primary-key "
                        f"index on column(s) `{cols}`"
                    ),
                    affected_table=diff.table_name,
                    affected_columns=list(idx.columns),
                    suggested_action=(
                        "Primary key columns are already indexed automatically by "
                        "PostgreSQL. Remove this redundant index to save storage "
                        "and reduce write overhead."
                    ),
                    context={
                        "index_name": idx.name,
                        "class_name": diff.class_name,
                        "line_number": idx.line or None,
                    },
                    cause_file=source_file,
                    cause_line=idx.line or None,
                    cause_context=f"redundant index {index_label} added here",
                )
            )

    return issues


# ---------------------------------------------------------------------------
# Convenience wrapper
# ---------------------------------------------------------------------------


def run_model_issue_detectors(
    diff: ModelDiff,
    schema: SchemaInfo | None = None,
    project_config: PgReviewerConfig | None = None,
    runtime_settings: Settings | None = None,
    source_file: str | None = None,
) -> list[Issue]:
    """Run all model-diff issue detectors against *diff* and return all issues.

    Parameters
    ----------
    diff:
        The :class:`~pgreviewer.parsing.model_differ.ModelDiff` to inspect.
    schema:
        Optional live schema information used by :func:`detect_removed_index`.
        When ``None``, an empty :class:`SchemaInfo` is used.

    Returns
    -------
    list[Issue]
        Combined list of issues from all detectors.
    """
    _schema = schema if schema is not None else SchemaInfo()
    issues: list[Issue] = []
    issues.extend(detect_missing_fk_index(diff, source_file))
    issues.extend(detect_removed_index(diff, _schema, source_file))
    issues.extend(detect_large_text_without_constraint(diff, source_file))
    issues.extend(detect_duplicate_pk_index(diff, source_file))
    return apply_issue_config(
        issues,
        project=project_config,
        runtime_settings=runtime_settings,
    )
