from __future__ import annotations

import contextlib
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pgreviewer.reporting.pr_comment import REPORT_SIGNATURE

# Regex to extract SQL from suggested_action fields
_SQL_FROM_ACTION_RE = re.compile(
    r"(?:blocking writers|Suggested SQL|suggested fix|replace with)"
    r":\s*(.+?)(?:\.\s*Note:|$)",
    re.IGNORECASE | re.DOTALL,
)

_GITHUB_API_BASE = "https://api.github.com"
_NEXT_LINK_RE = re.compile(r'<([^>]+)>;\s*rel="next"')

# Embedded in the review body so we can find and clean up old reviews.
# Format: <!-- pgreviewer-review sha:{commit_sha} -->
_REVIEW_SIGNATURE_PREFIX = "<!-- pgreviewer-review"


def _github_request(
    method: str,
    url: str,
    token: str,
    payload: dict[str, Any] | None = None,
) -> tuple[Any, dict[str, str]]:
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "pgreviewer",
    }
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = Request(url=url, data=data, headers=headers, method=method)
    try:
        with urlopen(request) as response:  # noqa: S310
            raw = response.read().decode("utf-8")
            parsed = json.loads(raw) if raw else None
            return parsed, dict(response.headers.items())
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub API request failed ({exc.code}): {detail}") from exc


def _extract_next_link(link_header: str | None) -> str | None:
    if not link_header:
        return None
    match = _NEXT_LINK_RE.search(link_header)
    return match.group(1) if match else None


def find_existing_comment(pr_number: int, repo: str, token: str) -> int | None:
    url: str | None = (
        f"{_GITHUB_API_BASE}/repos/{repo}/issues/{pr_number}/comments?per_page=100"
    )
    existing_comment_id: int | None = None

    while url is not None:
        payload, headers = _github_request("GET", url, token)
        if not isinstance(payload, list):
            return existing_comment_id

        for comment in payload:
            if not isinstance(comment, dict):
                continue
            body = comment.get("body")
            comment_id = comment.get("id")
            if (
                isinstance(body, str)
                and REPORT_SIGNATURE in body
                and isinstance(comment_id, int)
            ):
                existing_comment_id = comment_id

        url = _extract_next_link(headers.get("Link"))

    return existing_comment_id


def post_or_update_comment(pr_number: int, repo: str, token: str, body: str) -> None:
    existing_comment_id = find_existing_comment(
        pr_number=pr_number, repo=repo, token=token
    )
    payload = {"body": body}
    if existing_comment_id is None:
        _github_request(
            "POST",
            f"{_GITHUB_API_BASE}/repos/{repo}/issues/{pr_number}/comments",
            token,
            payload=payload,
        )
        return

    _github_request(
        "PATCH",
        f"{_GITHUB_API_BASE}/repos/{repo}/issues/comments/{existing_comment_id}",
        token,
        payload=payload,
    )


# ── Inline PR review with suggestion blocks ───────────────────────────────────

_SEV_ICON = {"CRITICAL": "🔴", "WARNING": "🟡", "INFO": "ℹ️"}

_INDEX_DETECTORS = frozenset(
    {"create_index_not_concurrently", "drop_index_not_concurrently"}
)

_DETECTOR_WHY = {
    "create_index_not_concurrently": (
        "Without `CONCURRENTLY`, Postgres holds an `AccessExclusiveLock` for the "
        "entire index build — blocking **all** reads and writes on this table."
    ),
    "drop_index_not_concurrently": (
        "Without `CONCURRENTLY`, `DROP INDEX` holds an `AccessExclusiveLock` for "
        "the entire operation — blocking **all** reads and writes on this table."
    ),
    "add_foreign_key_without_index": (
        "FK columns without indexes trigger a full seq-scan on every join, "
        "cascade check, and ON DELETE operation."
    ),
    "add_column_with_default": (
        "Adding a column with a non-volatile DEFAULT rewrites the entire table "
        "on Postgres < 11, or for volatile defaults on any version."
    ),
    "destructive_ddl": (
        "Dropping a table or column is irreversible and will immediately break "
        "any code that still references it."
    ),
    "alter_column_type": (
        "Column type changes usually rewrite the entire table under an "
        "`AccessExclusiveLock`, blocking all traffic."
    ),
    "add_not_null_without_default": (
        "Adding NOT NULL validates every existing row under `AccessExclusiveLock`. "
        "Use NOT VALID + VALIDATE CONSTRAINT in a separate step instead."
    ),
}


_CREATE_INDEX_PARSE_RE = re.compile(
    r"CREATE\s+(?P<unique>UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?"
    r"(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>\S+)\s+ON\s+(?P<table>\S+)"
    r"\s*\((?P<cols>[^)]+)\)",
    re.IGNORECASE,
)
_DROP_INDEX_PARSE_RE = re.compile(
    r"DROP\s+INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+EXISTS\s+)?(?P<name>[^\s;,]+)",
    re.IGNORECASE,
)


def _sql_to_autocommit_block(sql: str, indent: str) -> str | None:
    """Convert CREATE/DROP INDEX SQL to Alembic autocommit_block() pattern."""
    s = sql.strip().rstrip(";")
    upper = s.upper()
    inner = indent + "    "

    if "CREATE" in upper and "INDEX" in upper:
        m = _CREATE_INDEX_PARSE_RE.search(s)
        if not m:
            return None
        is_unique = bool(m.group("unique"))
        name = m.group("name").strip('"')
        table = m.group("table").strip('"')
        cols = [c.strip().strip('"') for c in m.group("cols").split(",")]
        col_repr = ", ".join(f'"{c}"' for c in cols)
        unique_line = f"\n{inner}    unique=True," if is_unique else ""
        return (
            f"{indent}with op.get_context().autocommit_block():\n"
            f"{inner}op.create_index(\n"
            f'{inner}    "{name}",\n'
            f'{inner}    "{table}",\n'
            f"{inner}    [{col_repr}],{unique_line}\n"
            f"{inner}    postgresql_concurrently=True,\n"
            f"{inner}    if_not_exists=True,\n"
            f"{inner})"
        )

    if "DROP" in upper and "INDEX" in upper:
        m = _DROP_INDEX_PARSE_RE.search(s)
        if not m:
            return None
        name = m.group("name").strip('"')
        return (
            f"{indent}with op.get_context().autocommit_block():\n"
            f"{inner}op.drop_index(\n"
            f'{inner}    "{name}",\n'
            f"{inner}    postgresql_concurrently=True,\n"
            f"{inner}    if_exists=True,\n"
            f"{inner})"
        )

    return None


def _read_file_line(file_path: str, line_number: int) -> str | None:
    """Read a specific line from a file relative to cwd. Returns None if unreadable."""
    try:
        p = Path(file_path)
        if not p.is_absolute():
            p = Path.cwd() / p
        lines = p.read_text(encoding="utf-8").splitlines()
        if 1 <= line_number <= len(lines):
            return lines[line_number - 1]
    except Exception:
        pass
    return None


def _extract_fix_sql(suggested_action: str) -> str | None:
    m = _SQL_FROM_ACTION_RE.search(suggested_action)
    if not m:
        return None
    sql = m.group(1).strip().rstrip(";")
    if sql.upper().startswith(("CREATE", "ALTER", "DROP", "INSERT", "UPDATE")):
        return sql + ";"
    return None


def _make_suggestion_body(
    detector: str,
    severity: str,
    suggested_action: str,
    source_line: str | None,
) -> str:
    """Build the markdown body for an inline review comment."""
    icon = _SEV_ICON.get(severity, "ℹ️")
    why = _DETECTOR_WHY.get(detector, "")
    fix_sql = _extract_fix_sql(suggested_action)

    lines = [f"**{icon} `{detector}`**", ""]

    if why:
        lines += [f"> {why}", ""]

    # For index detectors: use autocommit_block() pattern (idiomatic Alembic)
    if fix_sql and source_line is not None and detector in _INDEX_DETECTORS:
        indent = " " * (len(source_line) - len(source_line.lstrip()))
        block = _sql_to_autocommit_block(fix_sql, indent)
        if block:
            lines += [
                "Replace with `autocommit_block()` pattern:",
                "",
                "```suggestion",
                block,
                "```",
                "",
                "> ⚠️ `CONCURRENTLY` cannot run inside a transaction block. "
                "Wrap in `op.get_context().autocommit_block()` or split "
                "into a separate non-transactional migration file.",
            ]
            return "\n".join(lines)

    # For other detectors: wrap raw SQL in op.execute()
    if fix_sql and source_line is not None:
        indent = len(source_line) - len(source_line.lstrip())
        prefix = " " * indent
        suggestion_line = f'{prefix}op.execute("{fix_sql}")'
        lines += [
            "Replace with `op.execute()` + `CONCURRENTLY` "
            "(must run outside a transaction):",
            "",
            "```suggestion",
            suggestion_line,
            "```",
            "",
            "> ⚠️ `CONCURRENTLY` cannot run inside a transaction block. "
            "Use `op.execute()` directly and ensure this migration is "
            "non-transactional (or split into a separate migration file).",
        ]
    elif fix_sql:
        lines += ["**Suggested fix:**", "", "```sql", fix_sql, "```"]
    elif suggested_action:
        lines += [f"> {suggested_action}"]

    return "\n".join(lines)


def _find_pgreviewer_reviews(
    pr_number: int, repo: str, token: str
) -> list[dict[str, Any]]:
    """Return all pgReviewer reviews on the PR across all pages."""
    url: str | None = (
        f"{_GITHUB_API_BASE}/repos/{repo}/pulls/{pr_number}/reviews?per_page=100"
    )
    found: list[dict[str, Any]] = []
    while url is not None:
        payload, headers = _github_request("GET", url, token)
        if not isinstance(payload, list):
            break
        for review in payload:
            body = review.get("body") or ""
            if _REVIEW_SIGNATURE_PREFIX in body:
                found.append(review)
        url = _extract_next_link(headers.get("Link"))
    return found


def _delete_review_comments(
    pr_number: int, repo: str, token: str, review_id: int
) -> None:
    """Delete every inline comment belonging to a review (parallel)."""
    # Collect all IDs first (may be paginated)
    comment_ids: list[int] = []
    url: str | None = (
        f"{_GITHUB_API_BASE}/repos/{repo}/pulls/{pr_number}"
        f"/reviews/{review_id}/comments"
    )
    while url is not None:
        payload, headers = _github_request("GET", url, token)
        if not isinstance(payload, list):
            break
        for comment in payload:
            cid = comment.get("id")
            if cid:
                comment_ids.append(cid)
        url = _extract_next_link(headers.get("Link"))

    if not comment_ids:
        return

    def _delete(comment_id: int) -> None:
        with contextlib.suppress(RuntimeError):
            _github_request(
                "DELETE",
                f"{_GITHUB_API_BASE}/repos/{repo}/pulls/comments/{comment_id}",
                token,
            )

    with ThreadPoolExecutor(max_workers=min(10, len(comment_ids))) as pool:
        list(pool.map(_delete, comment_ids))


def post_review_with_suggestions(
    pr_number: int,
    repo: str,
    token: str,
    report: dict[str, Any],
    commit_sha: str,
) -> None:
    """
    Post a GitHub PR review with inline suggestion comments.

    For each issue in the diff report that has a file + line number,
    posts an inline comment on that exact line with:
    - What the issue is and why it matters
    - A `suggestion` block with the fixed code (one-click apply)

    GitHub limits review comment submissions to 64 comments per review;
    we cap at 50 to stay safe.

    TODO: batch index suggestions per upgrade/downgrade function.
    When multiple CREATE INDEX / DROP INDEX issues exist in the same
    function, the ideal suggestion is a single autocommit_block() wrapping
    all of them — rather than one per-line suggestion each with its own
    block. Implementing this requires:
      1. Storing function context (upgrade/downgrade) on each Issue during
         extraction (re-parse the file to find which function owns the line).
      2. Grouping issues by (source_file, function_name) in this reporter.
      3. Using GitHub's start_line + line range for a single multi-line
         suggestion comment instead of N single-line comments.
    """
    comments: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()  # deduplicate (file, line) pairs

    for result in report.get("results", []):
        source_file = result.get("source_file", "")
        line_number = result.get("line_number")
        if not source_file or not line_number:
            continue

        for issue in result.get("issues", []):
            key = (source_file, line_number)
            if key in seen:
                continue
            seen.add(key)

            detector = issue.get("detector_name", "")
            severity = issue.get("severity", "INFO")
            suggested_action = issue.get("suggested_action", "")

            source_line = _read_file_line(source_file, line_number)
            body = _make_suggestion_body(
                detector, severity, suggested_action, source_line
            )

            comments.append(
                {
                    "path": source_file,
                    "line": line_number,
                    "side": "RIGHT",
                    "body": body,
                }
            )

            if len(comments) >= 50:
                break
        if len(comments) >= 50:
            break

    if not comments:
        return

    # Deduplicate across CI re-runs:
    # - If a review for this exact commit already exists → skip (idempotent).
    # - If reviews for older commits exist → delete their inline comments so
    #   they don't accumulate as stale suggestions on the PR.
    try:
        existing = _find_pgreviewer_reviews(pr_number, repo, token)
        for review in existing:
            body = review.get("body") or ""
            if f"sha:{commit_sha}" in body:
                print(f"Inline review already posted for {commit_sha[:8]}, skipping.")
                return
            review_id = review.get("id")
            if review_id:
                _delete_review_comments(pr_number, repo, token, review_id)
    except RuntimeError as exc:
        print(f"Warning: could not clean up old reviews: {exc}", file=sys.stderr)

    review_body = f"{_REVIEW_SIGNATURE_PREFIX} sha:{commit_sha} -->"
    try:
        _github_request(
            "POST",
            f"{_GITHUB_API_BASE}/repos/{repo}/pulls/{pr_number}/reviews",
            token,
            payload={
                "commit_id": commit_sha,
                "event": "COMMENT",
                "body": review_body,
                "comments": comments,
            },
        )
        print(f"Posted PR review with {len(comments)} inline suggestion(s).")
    except RuntimeError as exc:
        print(f"Warning: could not post inline review: {exc}", file=sys.stderr)
