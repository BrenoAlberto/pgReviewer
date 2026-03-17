from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pgreviewer.core.models import Issue, Severity
from pgreviewer.parsing.sqlalchemy_analyzer import analyze_model_file

if TYPE_CHECKING:
    from pgreviewer.parsing.sqlalchemy_analyzer import ModelDefinition


def check_models_in_path(path: str | Path) -> list[Issue]:
    """Scan all Python files in the given path for static model issues."""
    path_obj = Path(path)
    if not path_obj.exists():
        raise FileNotFoundError(f"Path not found: {path}")

    issues: list[Issue] = []

    # Find all .py files
    if path_obj.is_file():
        files = [path_obj] if path_obj.suffix == ".py" else []
    else:
        files = list(path_obj.rglob("*.py"))

    for f in files:
        try:
            models = analyze_model_file(f, include_abstract=True)
            for m in models:
                issues.extend(_check_model(m, f))
        except Exception:
            # Optionally log, but for now just skip unparseable files
            pass

    return issues


def _check_model(model: ModelDefinition, filepath: Path) -> list[Issue]:
    issues: list[Issue] = []
    # If the class has columns but no __tablename__, flag it (unless it's named like
    # an abstract/mixin).
    # A simple heuristic: if it has at least one column mapped and no table name,
    # and its name doesn't end with "Mixin" and it's not "Base".
    has_columns = len(model.columns) > 0
    if (
        not model.table_name
        and has_columns
        and (
            model.class_name not in ("Base", "DeclarativeBase")
            and not model.class_name.endswith("Mixin")
        )
    ):
        issues.append(
            Issue(
                severity=Severity.CRITICAL,
                detector_name="MissingTablename",
                description=(
                    f"Model class '{model.class_name}' defines columns "
                    "but is missing '__tablename__'."
                ),
                affected_table=model.class_name,
                affected_columns=[],
                suggested_action="Add '__tablename__ = \"...\"' to the class.",
                context={"file": str(filepath)},
            )
        )

    # Missing FK indexes
    # Check if a column has an FK. If it does, does it have an index?
    # SQLAlchemy models: column.index might be True, or there might be an explicit
    # Index on it.
    indexed_columns = set()
    for col in model.columns:
        if col.index or col.unique or col.primary_key:
            indexed_columns.add(col.name)

    for idx in model.indexes:
        # For simplicity, if a column is part of any index, we count it as indexed.
        for c in idx.columns:
            indexed_columns.add(c)

    for fk in model.foreign_keys:
        if fk.column_name not in indexed_columns:
            issues.append(
                Issue(
                    severity=Severity.WARNING,
                    detector_name="MissingFKIndex",
                    description=(
                        f"Foreign key column '{fk.column_name}' is missing an index."
                    ),
                    affected_table=model.table_name or model.class_name,
                    affected_columns=[fk.column_name],
                    suggested_action=(
                        f"Add 'index=True' to the mapped_column for '{fk.column_name}'."
                    ),
                    context={"file": str(filepath)},
                )
            )

    # Heuristic for common filter patterns (like 'status', 'email', 'created_at')
    # that typically require an index.
    common_filter_cols = {"status", "email", "created_at", "type", "category"}
    for col in model.columns:
        if col.name in common_filter_cols and col.name not in indexed_columns:
            # We flag this as INFO or WARNING since it's a common pattern
            issues.append(
                Issue(
                    severity=Severity.INFO,
                    detector_name="MissingCommonFilterIndex",
                    description=(
                        f"Column '{col.name}' is often used in filters "
                        "but lacks an index."
                    ),
                    affected_table=model.table_name or model.class_name,
                    affected_columns=[col.name],
                    suggested_action=(
                        f"Add 'index=True' to the mapped_column for '{col.name}'."
                    ),
                    context={"file": str(filepath)},
                )
            )

    return issues
