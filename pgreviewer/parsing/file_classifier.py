from enum import StrEnum
from fnmatch import fnmatch

from pgreviewer.config import settings

# Migration directory segments that indicate a file is part of a DB migration.
_MIGRATION_SEGMENTS = ("migrations/", "alembic/versions/", "flyway/")

# Python SQL execution markers.
_PYTHON_SQL_MARKERS = (
    "op.execute(",
    "cursor.execute(",
    "text(",
    "session.execute(",
)


class FileType(StrEnum):
    MIGRATION_SQL = "MIGRATION_SQL"
    MIGRATION_PYTHON = "MIGRATION_PYTHON"
    RAW_SQL = "RAW_SQL"
    PYTHON_WITH_SQL = "PYTHON_WITH_SQL"
    IGNORE = "IGNORE"


def _is_ignored(path: str) -> bool:
    """Return True if *path* matches any pattern in ``settings.IGNORE_PATHS``."""
    return any(fnmatch(path, pattern) for pattern in settings.IGNORE_PATHS)


def _in_migration_dir(path: str) -> bool:
    """Return True if *path* lives inside a recognised migration directory."""
    normalised = path.replace("\\", "/")
    return any(segment in normalised for segment in _MIGRATION_SEGMENTS)


def _has_sql_markers(content: str) -> bool:
    """Return True if *content* contains any Python SQL execution marker."""
    return any(marker in content for marker in _PYTHON_SQL_MARKERS)


def classify_file(path: str, content: str) -> FileType:
    """Classify a changed file so that the reviewer knows how to handle it.

    Args:
        path: Relative path of the file inside the repository.
        content: Full text content of the file (used for Python SQL detection).

    Returns:
        A :class:`FileType` member describing the kind of file.

    Classification order
    --------------------
    1. Paths matching ``settings.IGNORE_PATHS`` glob patterns → :attr:`FileType.IGNORE`.
    2. Files inside a migration directory (``migrations/``, ``alembic/versions/``,
       ``flyway/``):

       - ``.py`` extension → :attr:`FileType.MIGRATION_PYTHON`
       - anything else (typically ``.sql``) → :attr:`FileType.MIGRATION_SQL`

    3. ``.sql`` files outside a migration directory → :attr:`FileType.RAW_SQL`.
    4. ``.py`` files containing a SQL execution marker
       → :attr:`FileType.PYTHON_WITH_SQL`.
    5. All remaining files → :attr:`FileType.IGNORE`.
    """
    if _is_ignored(path):
        return FileType.IGNORE

    if _in_migration_dir(path):
        if path.endswith(".py"):
            return FileType.MIGRATION_PYTHON
        return FileType.MIGRATION_SQL

    if path.endswith(".sql"):
        return FileType.RAW_SQL

    if path.endswith(".py") and _has_sql_markers(content):
        return FileType.PYTHON_WITH_SQL

    return FileType.IGNORE
