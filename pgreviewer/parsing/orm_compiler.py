"""ORM-to-SQL compiler using SQLAlchemy's own ``.compile()`` method.

Best-effort: adds the project's ``src/`` directory to ``sys.path`` and tries
to import its SQLAlchemy models so that ORM query code can be executed and
compiled to PostgreSQL-dialect SQL.

This module is intentionally defensive — any import or execution failure is
caught and logged at INFO level so that callers can fall back gracefully to
the approximate SQL produced by the tree-sitter AST extractor.

Confidence levels
-----------------
- ``0.95`` — returned SQL was produced by SQLAlchemy's own compiler.
- ``0.7``  — caller fell back to AST approximation (not produced here).

Public API
----------
- :func:`compile_orm_query` — attempt to compile an ORM query expression
  to PostgreSQL-dialect SQL.
"""

from __future__ import annotations

import contextlib
import logging
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

#: Confidence assigned to queries compiled successfully by SQLAlchemy.
COMPILED_CONFIDENCE: float = 0.95

#: Confidence assigned when falling back to the AST approximation.
FALLBACK_CONFIDENCE: float = 0.7


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _add_project_paths(project_path: Path) -> list[str]:
    """Prepend ``<project_path>/src`` and ``<project_path>`` to ``sys.path``.

    Returns the list of paths that were actually inserted (so they can be
    removed again after compilation).
    """
    added: list[str] = []
    for candidate in (project_path / "src", project_path):
        s = str(candidate.resolve())
        if candidate.is_dir() and s not in sys.path:
            sys.path.insert(0, s)
            added.append(s)
    return added


def _remove_paths(paths: list[str]) -> None:
    """Remove *paths* from ``sys.path`` (best-effort; ignores missing entries)."""
    for p in paths:
        with contextlib.suppress(ValueError):
            sys.path.remove(p)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compile_orm_query(
    query_source: str,
    project_path: str | Path | None = None,
    *,
    extra_namespace: dict[str, Any] | None = None,
) -> str | None:
    """Compile an ORM query expression to SQL via SQLAlchemy's compiler.

    The function executes *query_source* (a Python expression) inside a
    controlled namespace that includes common SQLAlchemy symbols and any
    caller-supplied names.  The resulting object's ``.compile()`` method is
    called with the PostgreSQL dialect and ``literal_binds=True`` to produce
    a complete SQL string.

    This is **best-effort**: any failure (missing imports, circular
    dependencies, unresolved bind parameters, …) is caught, logged at INFO
    level, and ``None`` is returned so that callers can fall back to the
    approximate SQL from the AST extractor.

    Parameters
    ----------
    query_source:
        Python expression that evaluates to a SQLAlchemy ``Query`` or
        ``Select`` (or any compilable ``ClauseElement``), e.g.
        ``"select(User).where(User.id == 1)"``.
    project_path:
        Optional path to the project root.  When provided,
        ``<project_path>/src`` and ``<project_path>`` are temporarily
        prepended to ``sys.path`` so that the project's model modules can be
        imported by *extra_namespace* setup or by code inside *query_source*.
    extra_namespace:
        Optional mapping of names to inject into the execution namespace
        before evaluating *query_source*.  Useful for passing model classes
        and other symbols that the expression references.

    Returns
    -------
    str or None
        The compiled PostgreSQL-dialect SQL string on success; ``None`` if
        compilation fails for any reason.  Failure is always logged at INFO
        level — this function never raises.
    """
    try:
        from sqlalchemy.dialects import postgresql
    except ImportError:
        logger.info(
            "compile_orm_query: sqlalchemy is not installed; skipping ORM compilation"
        )
        return None

    added_paths: list[str] = []
    if project_path is not None:
        added_paths = _add_project_paths(Path(project_path))

    try:
        # Build the execution namespace with common SQLAlchemy symbols so
        # that simple expressions work without explicit imports.
        namespace: dict[str, Any] = {}
        try:
            import sqlalchemy as sa
            from sqlalchemy import select

            namespace["sa"] = sa
            namespace["select"] = select
        except ImportError:
            pass

        if extra_namespace:
            namespace.update(extra_namespace)

        # Evaluate the query expression and capture the result.
        # eval() is used (rather than exec) because query_source must be a
        # Python *expression*, not a statement. This also limits what can be
        # executed: eval() cannot run multi-statement code.
        query_obj = eval(query_source, namespace)  # noqa: S307

        if query_obj is None:
            logger.info(
                "compile_orm_query: expression %r evaluated to None", query_source
            )
            return None

        # Obtain a compilable ClauseElement.
        # Legacy Query objects expose `.statement`; Core Select objects are
        # directly compilable.
        stmt = getattr(query_obj, "statement", query_obj)

        compiled = stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
        return str(compiled)

    except Exception as exc:  # noqa: BLE001
        logger.info(
            "compile_orm_query: compilation failed (%s: %s)",
            type(exc).__name__,
            exc,
        )
        return None

    finally:
        _remove_paths(added_paths)
