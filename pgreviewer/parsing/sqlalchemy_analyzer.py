"""SQLAlchemy declarative-model analyzer using tree-sitter.

Parses Python source files that define SQLAlchemy ORM models (classes that
inherit from ``Base`` or ``DeclarativeBase``) and returns typed
``ModelDefinition`` objects describing the schema without requiring a live
database connection.

Tree-sitter queries are stored in::

    pgreviewer/parsing/treesitter/queries/python/sqlalchemy_models.scm

Post-processing in this module converts raw AST captures into the typed
dataclasses below.

Public API
----------
- :func:`analyze_model_file` – analyse a file by path
- :func:`analyze_model_source` – analyse raw source text (useful for tests)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import tree_sitter_python as tspython
from tree_sitter import Language, Node, Parser, Query, QueryCursor

logger = logging.getLogger(__name__)

PY_LANGUAGE = Language(tspython.language())
_parser = Parser(PY_LANGUAGE)

_QUERIES_DIR = Path(__file__).parent / "treesitter" / "queries" / "python"
_SCM_FILE = _QUERIES_DIR / "sqlalchemy_models.scm"

# Base-class names that mark a class as a SQLAlchemy model.
_MODEL_BASES: frozenset[str] = frozenset(
    {"Base", "DeclarativeBase", "DeclarativeBaseNoMeta"}
)


# ---------------------------------------------------------------------------
# Typed output dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ColumnDef:
    """A single column defined inside a SQLAlchemy model class."""

    name: str
    col_type: str  # e.g. "Integer", "String", "DateTime"
    nullable: bool = True
    index: bool = False
    unique: bool = False
    primary_key: bool = False
    has_type_args: bool = False  # True when type has arguments, e.g. String(50)
    line: int = 0


@dataclass
class FKDef:
    """A ForeignKey reference extracted from a Column definition."""

    column_name: str  # the column that owns this FK
    target: str  # e.g. "users.id"
    line: int = 0


@dataclass
class IndexDef:
    """An explicit ``Index(...)`` definition."""

    name: str | None  # index name (first positional string arg)
    columns: list[str]  # column name strings passed as positional args
    is_unique: bool = False
    line: int = 0


@dataclass
class RelDef:
    """A ``relationship(...)`` definition."""

    name: str  # attribute name
    target_model: str  # first positional arg, e.g. "User"
    back_populates: str | None = None
    foreign_keys: list[str] = field(default_factory=list)
    line: int = 0


@dataclass
class ModelDefinition:
    """All schema information extracted from a single model class."""

    class_name: str
    table_name: str
    columns: list[ColumnDef] = field(default_factory=list)
    foreign_keys: list[FKDef] = field(default_factory=list)
    indexes: list[IndexDef] = field(default_factory=list)
    relationships: list[RelDef] = field(default_factory=list)
    line: int = 0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_query() -> Query:
    """Compile and return the cached SQLAlchemy tree-sitter query."""
    return Query(PY_LANGUAGE, _SCM_FILE.read_text())


def _unquote(node: Node) -> str:
    """Return the string value of a tree-sitter *string* node (strips quotes)."""
    raw = node.text.decode("utf-8")
    for q in ['"""', "'''", '"', "'"]:
        idx = raw.find(q)
        if idx != -1:
            content_start = idx + len(q)
            content_end = raw.rfind(q)
            if content_end > content_start:
                return raw[content_start:content_end]
            return raw[content_start:]
    return raw


def _get_col_type(args_node: Node) -> tuple[str, bool]:
    """Derive the SQLAlchemy column type from a Column() argument list node.

    Scans positional arguments (skips keyword arguments and ForeignKey calls)
    and returns a tuple of ``(col_type, has_type_args)`` where *has_type_args*
    is ``True`` when the type was specified with arguments (e.g. ``String(50)``).
    """
    for child in args_node.named_children:
        if child.type == "keyword_argument":
            continue
        if child.type == "call":
            func = child.child_by_field_name("function")
            if func and func.text.decode("utf-8") == "ForeignKey":
                continue
            # e.g. String(50) – use the function name as the type
            if func:
                return func.text.decode("utf-8"), True
        if child.type in ("identifier", "attribute"):
            return child.text.decode("utf-8"), False
    return "Unknown", False


def _get_fk_target(args_node: Node) -> str | None:
    """Return the ForeignKey target string from a Column() argument list, if present."""
    for child in args_node.named_children:
        if child.type == "call":
            func = child.child_by_field_name("function")
            if func and func.text.decode("utf-8") == "ForeignKey":
                fk_args = child.child_by_field_name("arguments")
                if fk_args and fk_args.named_children:
                    first = fk_args.named_children[0]
                    if first.type == "string":
                        return _unquote(first)
    return None


def _get_kwargs(args_node: Node) -> dict[str, str]:
    """Return all keyword arguments from an argument list node as ``{key: value}``."""
    result: dict[str, str] = {}
    for child in args_node.named_children:
        if child.type == "keyword_argument":
            key_node = child.child_by_field_name("name")
            val_node = child.child_by_field_name("value")
            if key_node and val_node:
                result[key_node.text.decode("utf-8")] = val_node.text.decode("utf-8")
    return result


def _parse_column(name_node: Node, args_node: Node) -> ColumnDef:
    """Build a :class:`ColumnDef` from the column name node and its argument list."""
    col_name = name_node.text.decode("utf-8")
    col_type, has_type_args = _get_col_type(args_node)
    fk_target = _get_fk_target(args_node)
    kwargs = _get_kwargs(args_node)

    def _bool_kwarg(key: str) -> bool:
        val = kwargs.get(key, "False")
        return val.lower() in ("true", "1")

    col_def = ColumnDef(
        name=col_name,
        col_type=col_type,
        nullable=(_bool_kwarg("nullable") if "nullable" in kwargs else True),
        index=_bool_kwarg("index"),
        unique=_bool_kwarg("unique"),
        primary_key=_bool_kwarg("primary_key"),
        has_type_args=has_type_args,
        line=name_node.start_point[0] + 1,
    )
    # Attach FK target directly so callers can check it
    if fk_target is not None:
        col_def.__dict__["_fk_target"] = fk_target
    return col_def


def _parse_relationship(name_node: Node, args_node: Node) -> RelDef:
    """Build a :class:`RelDef` from the relationship name node and its argument list."""
    rel_name = name_node.text.decode("utf-8")
    kwargs = _get_kwargs(args_node)

    # First positional string arg is the target model name
    target_model = ""
    for child in args_node.named_children:
        if child.type == "keyword_argument":
            continue
        if child.type == "string":
            target_model = _unquote(child)
            break

    back_populates: str | None = None
    if "back_populates" in kwargs:
        raw = kwargs["back_populates"]
        back_populates = _unquote_raw(raw)

    # foreign_keys=[col1, col2] – extract identifier names
    fk_list: list[str] = []
    if "foreign_keys" in kwargs:
        raw_fks = kwargs["foreign_keys"]
        # strip the enclosing list brackets if present
        if raw_fks.startswith("[") and raw_fks.endswith("]"):
            stripped = raw_fks[1:-1]
        else:
            stripped = raw_fks
        fk_list = [s.strip() for s in stripped.split(",") if s.strip()]

    return RelDef(
        name=rel_name,
        target_model=target_model,
        back_populates=back_populates,
        foreign_keys=fk_list,
        line=name_node.start_point[0] + 1,
    )


def _unquote_raw(raw: str) -> str:
    """Strip surrounding quotes from a raw string literal value."""
    for q in ['"""', "'''", '"', "'"]:
        if raw.startswith(q) and raw.endswith(q) and len(raw) >= 2 * len(q):
            return raw[len(q) : -len(q)]
    return raw


def _parse_index(args_node: Node) -> IndexDef | None:
    """Build an :class:`IndexDef` from an ``Index(...)`` argument list node.

    Returns ``None`` if the argument list contains no string column references.
    """
    kwargs = _get_kwargs(args_node)
    is_unique = kwargs.get("unique", "False").lower() in ("true", "1")

    name: str | None = None
    columns: list[str] = []

    for child in args_node.named_children:
        if child.type == "keyword_argument":
            continue
        if child.type == "string":
            val = _unquote(child)
            if name is None:
                name = val  # first string is the index name
            else:
                columns.append(val)  # subsequent strings are column names

    if name is None and not columns:
        return None

    return IndexDef(
        name=name,
        columns=columns,
        is_unique=is_unique,
        line=args_node.start_point[0] + 1,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def analyze_model_source(
    source: str, file_path: str = "", include_abstract: bool = False
) -> list[ModelDefinition]:
    """Analyse *source* text and return one :class:`ModelDefinition` per model class.

    Only classes that inherit from one of the recognised base-class names
    (``Base``, ``DeclarativeBase``, ``DeclarativeBaseNoMeta``) **and** declare
    ``__tablename__`` are included in the result (unless ``include_abstract`` is True).

    Parameters
    ----------
    source:
        Raw Python source text of the file to analyse.
    file_path:
        Optional path used only for log messages.
    include_abstract:
        If True, includes models that do not define a __tablename__.

    Returns
    -------
    list[ModelDefinition]
        One entry per model class found, in source order.
    """
    if not source.strip():
        return []

    tree = _parser.parse(source.encode("utf-8"))
    query = _load_query()
    cursor = QueryCursor(query)
    all_matches: list[tuple[int, dict[str, list[Node]]]] = cursor.matches(
        tree.root_node
    )

    # ------------------------------------------------------------------
    # Pass 1: collect model class definitions + body byte-ranges
    # ------------------------------------------------------------------
    # class_name -> ModelDefinition (incomplete – filled in pass 2)
    models: dict[str, ModelDefinition] = {}
    # class_name -> (body_start_byte, body_end_byte)
    class_ranges: dict[str, tuple[int, int]] = {}

    for _pattern_idx, caps in all_matches:
        if "class_name" not in caps:
            continue
        class_name_node = caps["class_name"][0]
        base_nodes = caps.get("base_class", [])
        if not any(bn.text.decode("utf-8") in _MODEL_BASES for bn in base_nodes):
            continue

        class_name = class_name_node.text.decode("utf-8")
        if class_name in models:
            continue  # already registered (multiple base-class matches)

        class_def_node = class_name_node.parent  # class_definition
        body_node = class_def_node.child_by_field_name("body")
        if body_node is None:
            continue

        class_ranges[class_name] = (body_node.start_byte, body_node.end_byte)
        models[class_name] = ModelDefinition(
            class_name=class_name,
            table_name="",
            line=class_name_node.start_point[0] + 1,
        )

    if not models:
        return []

    def _find_class(node: Node) -> str | None:
        """Return the class name whose body contains *node*, or None."""
        nb = node.start_byte
        for cn, (start, end) in class_ranges.items():
            if start <= nb < end:
                return cn
        return None

    # ------------------------------------------------------------------
    # Pass 2: process remaining captures and associate with their class
    # ------------------------------------------------------------------
    for _pattern_idx, caps in all_matches:
        # ---- __tablename__ ----------------------------------------
        if "tablename_attr" in caps:
            tv_nodes = caps.get("tablename_value", [])
            if not tv_nodes:
                continue
            class_name = _find_class(caps["tablename_attr"][0])
            if class_name:
                models[class_name].table_name = _unquote(tv_nodes[0])

        # ---- Column -----------------------------------------------
        elif "col_name" in caps and "col_args" in caps:
            col_name_node = caps["col_name"][0]
            col_args_node = caps["col_args"][0]
            class_name = _find_class(col_name_node)
            if not class_name:
                continue
            col_def = _parse_column(col_name_node, col_args_node)
            models[class_name].columns.append(col_def)
            fk_target = col_def.__dict__.pop("_fk_target", None)
            if fk_target:
                models[class_name].foreign_keys.append(
                    FKDef(
                        column_name=col_def.name,
                        target=fk_target,
                        line=col_def.line,
                    )
                )

        # ---- relationship -----------------------------------------
        elif "rel_name" in caps and "rel_args" in caps:
            rel_name_node = caps["rel_name"][0]
            rel_args_node = caps["rel_args"][0]
            class_name = _find_class(rel_name_node)
            if not class_name:
                continue
            models[class_name].relationships.append(
                _parse_relationship(rel_name_node, rel_args_node)
            )

        # ---- Index ------------------------------------------------
        elif "idx_func" in caps and "idx_args" in caps:
            idx_args_node = caps["idx_args"][0]
            class_name = _find_class(idx_args_node)
            if not class_name:
                continue
            idx_def = _parse_index(idx_args_node)
            if idx_def:
                models[class_name].indexes.append(idx_def)

    # ------------------------------------------------------------------
    # Return only classes that have a __tablename__ (real model classes)
    # or all classes if include_abstract is True.
    # ------------------------------------------------------------------
    if include_abstract:
        result = list(models.values())
    else:
        result = [m for m in models.values() if m.table_name]
    result.sort(key=lambda m: m.line)
    return result


def analyze_model_file(
    file_path: str | Path, include_abstract: bool = False
) -> list[ModelDefinition]:
    """Analyse a Python file at *file_path* and return its model definitions.

    Parameters
    ----------
    file_path:
        Path to the ``.py`` file containing SQLAlchemy model classes.
    include_abstract:
        If True, includes models that do not define a __tablename__.

    Returns
    -------
    list[ModelDefinition]
        One entry per model class with ``__tablename__``, in source order.
    """
    path = Path(file_path)
    source = path.read_text(encoding="utf-8")
    return analyze_model_source(source, str(path), include_abstract=include_abstract)
