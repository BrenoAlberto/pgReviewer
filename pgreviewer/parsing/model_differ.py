"""Model diff: compare SQLAlchemy model definitions between two versions.

Performs a structural diff between two
:class:`~pgreviewer.parsing.sqlalchemy_analyzer.ModelDefinition` objects
(typically the base-branch version vs the PR-branch version) and returns a
:class:`ModelDiff` describing what was added or removed.

Public API
----------
- :func:`diff_models` – compare two :class:`ModelDefinition` objects
- :class:`ModelDiff` – result holding added/removed columns, indexes, relationships
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pgreviewer.parsing.sqlalchemy_analyzer import (
        ColumnDef,
        FKDef,
        IndexDef,
        ModelDefinition,
        RelDef,
    )


@dataclass
class ModelDiff:
    """Structural diff between two versions of a SQLAlchemy model class."""

    class_name: str
    table_name: str
    added_columns: list[ColumnDef] = field(default_factory=list)
    removed_columns: list[ColumnDef] = field(default_factory=list)
    added_indexes: list[IndexDef] = field(default_factory=list)
    removed_indexes: list[IndexDef] = field(default_factory=list)
    added_relationships: list[RelDef] = field(default_factory=list)
    removed_relationships: list[RelDef] = field(default_factory=list)
    added_foreign_keys: list[FKDef] = field(default_factory=list)
    removed_foreign_keys: list[FKDef] = field(default_factory=list)
    # PK column names from the *after* model (used by duplicate-index detector)
    pk_columns: list[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        """Return ``True`` when at least one structural field changed."""
        return bool(
            self.added_columns
            or self.removed_columns
            or self.added_indexes
            or self.removed_indexes
            or self.added_relationships
            or self.removed_relationships
        )


def diff_models(before: ModelDefinition, after: ModelDefinition) -> ModelDiff:
    """Compare two versions of a SQLAlchemy model and return a structural diff.

    Columns, named indexes, and relationships are matched by name.  Unnamed
    indexes (``IndexDef.name is None``) are compared by their column set.

    Parameters
    ----------
    before:
        The :class:`ModelDefinition` from the base branch.
    after:
        The :class:`ModelDefinition` from the PR branch.

    Returns
    -------
    ModelDiff
        Structural diff with added/removed columns, indexes, and relationships.
    """
    # --- columns -------------------------------------------------------
    before_cols = {c.name: c for c in before.columns}
    after_cols = {c.name: c for c in after.columns}
    added_columns = [after_cols[n] for n in after_cols if n not in before_cols]
    removed_columns = [before_cols[n] for n in before_cols if n not in after_cols]

    # --- indexes -------------------------------------------------------
    # Named indexes are matched by name; unnamed ones by their column set.
    before_named_idx = {i.name: i for i in before.indexes if i.name is not None}
    after_named_idx = {i.name: i for i in after.indexes if i.name is not None}
    added_indexes: list[IndexDef] = [
        after_named_idx[n] for n in after_named_idx if n not in before_named_idx
    ]
    removed_indexes: list[IndexDef] = [
        before_named_idx[n] for n in before_named_idx if n not in after_named_idx
    ]

    # Handle unnamed indexes via frozenset of columns
    before_unnamed = [frozenset(i.columns) for i in before.indexes if i.name is None]
    for idx in after.indexes:
        if idx.name is None and frozenset(idx.columns) not in before_unnamed:
            added_indexes.append(idx)
    after_unnamed = [frozenset(i.columns) for i in after.indexes if i.name is None]
    for idx in before.indexes:
        if idx.name is None and frozenset(idx.columns) not in after_unnamed:
            removed_indexes.append(idx)

    # --- relationships -------------------------------------------------
    before_rels = {r.name: r for r in before.relationships}
    after_rels = {r.name: r for r in after.relationships}
    added_relationships = [after_rels[n] for n in after_rels if n not in before_rels]
    removed_relationships = [before_rels[n] for n in before_rels if n not in after_rels]

    # --- foreign_keys --------------------------------------------------
    before_fks = {fk.column_name: fk for fk in before.foreign_keys}
    after_fks = {fk.column_name: fk for fk in after.foreign_keys}
    added_foreign_keys = [after_fks[n] for n in after_fks if n not in before_fks]
    removed_foreign_keys = [before_fks[n] for n in before_fks if n not in after_fks]

    # --- pk columns (from after model) ---------------------------------
    pk_columns = [c.name for c in after.columns if c.primary_key]

    return ModelDiff(
        class_name=after.class_name,
        table_name=after.table_name,
        added_columns=added_columns,
        removed_columns=removed_columns,
        added_indexes=added_indexes,
        removed_indexes=removed_indexes,
        added_relationships=added_relationships,
        removed_relationships=removed_relationships,
        added_foreign_keys=added_foreign_keys,
        removed_foreign_keys=removed_foreign_keys,
        pk_columns=pk_columns,
    )
