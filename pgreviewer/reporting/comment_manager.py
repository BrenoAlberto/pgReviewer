from __future__ import annotations

import contextlib
import json
import re
import sys
from collections import defaultdict
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
    "sql_injection_fstring": (
        "SQL built with f-string/string interpolation — an attacker who controls "
        "any interpolated value can read, modify, or delete arbitrary data."
    ),
    "query_in_loop": (
        "A database query is executed inside a loop — for N iterations this "
        "issues N round-trips to Postgres, dominating latency at any scale."
    ),
    "sqlalchemy_n_plus_one": (
        "A lazy-loaded SQLAlchemy relationship is accessed inside a loop, "
        "silently issuing one extra query per iteration."
    ),
    "missing_fk_index": (
        "PostgreSQL does not auto-create indexes on FK columns. Every join, "
        "cascade check, and ON DELETE on this column will do a full seq-scan."
    ),
    "removed_index": (
        "Removing this index will cause seq-scans on every query that relied on it. "
        "On large tables this can cause immediate latency regressions."
    ),
    "large_text_without_constraint": (
        "Unconstrained text columns have no length limit enforced by the database, "
        "which can lead to unexpectedly large rows and degraded query performance."
    ),
    "duplicate_pk_index": (
        "PostgreSQL automatically indexes primary key columns. "
        "This explicit index duplicates that implicit index, wasting storage and "
        "adding write overhead with no query-planning benefit."
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


_HUNK_HEADER_RE = re.compile(r"\+(\d+)(?:,\d+)?")


def _build_diff_index(diff_path: str) -> set[tuple[str, int]]:
    """Parse a unified diff and return commentable (file, line) pairs (RIGHT side).

    Only context and added lines can receive inline comments via the GitHub PR
    Review API. Removed lines are LEFT-side only.
    """
    index: set[tuple[str, int]] = set()
    try:
        content = Path(diff_path).read_text(encoding="utf-8", errors="replace")
    except Exception:
        return index

    current_file: str | None = None
    current_line = 0

    for raw_line in content.splitlines():
        if raw_line.startswith("+++ "):
            path = raw_line[4:]
            if path.startswith("b/"):
                path = path[2:]
            current_file = path if path != "/dev/null" else None
            current_line = 0
            continue

        if raw_line.startswith("--- "):
            continue

        if raw_line.startswith("@@"):
            m = _HUNK_HEADER_RE.search(raw_line)
            if m:
                current_line = int(m.group(1))
            continue

        if current_file is None or current_line == 0:
            continue

        if raw_line.startswith("-"):
            # Removed line — lives in old file only, not commentable on RIGHT side
            continue
        elif raw_line.startswith("+") or raw_line.startswith(" "):
            index.add((current_file, current_line))
            current_line += 1
        # Lines starting with "\" (no newline at end of file) — skip

    return index


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


def _read_file_lines(file_path: str, start: int, end: int) -> list[str] | None:
    """Read lines *start* through *end* (1-based, inclusive)."""
    try:
        p = Path(file_path)
        if not p.is_absolute():
            p = Path.cwd() / p
        all_lines = p.read_text(encoding="utf-8").splitlines()
        if 1 <= start <= end <= len(all_lines):
            return all_lines[start - 1 : end]
    except Exception:
        pass
    return None


def _find_call_span(file_path: str, content_line: int) -> tuple[int, int]:
    """Find the start and end lines of the enclosing call around *content_line*.

    Walks backwards from *content_line* to find the ``op.execute(`` (or
    similar call opening), then walks forward counting parentheses to find
    the matching close paren.

    Returns a (start, end) tuple of 1-based line numbers.  Falls back to
    ``(content_line, content_line)`` when the span cannot be determined.
    """
    try:
        p = Path(file_path)
        if not p.is_absolute():
            p = Path.cwd() / p
        lines = p.read_text(encoding="utf-8").splitlines()
    except Exception:
        return (content_line, content_line)

    # Walk backwards to find the call opening (line containing an unmatched '(')
    call_start = content_line
    for i in range(content_line - 1, max(content_line - 10, -1), -1):
        stripped = lines[i].lstrip()
        if "op.execute(" in stripped or "op.execute(text(" in stripped:
            call_start = i + 1  # 1-based
            break

    # Walk forward from call_start counting parens to find the close
    depth = 0
    for i in range(call_start - 1, min(call_start + 20, len(lines))):
        for ch in lines[i]:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return (call_start, i + 1)  # 1-based

    return (call_start, content_line)


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
    fix_type: str = "replace",
    *,
    original_lines: list[str] | None = None,
) -> str:
    """Build the markdown body for an inline review comment.

    Parameters
    ----------
    original_lines:
        For additive fixes, the full source lines of the statement being
        annotated.  When provided the suggestion block preserves these
        lines and appends the fix, giving the user a one-click commit.
    """
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

    # Additive fix with committable suggestion: preserve original code,
    # append the index in an autocommit_block() after the FK statement.
    if fix_sql and fix_type == "additive" and original_lines:
        indent = " " * (len(original_lines[0]) - len(original_lines[0].lstrip()))
        block = _sql_to_autocommit_block(fix_sql, indent)
        if block:
            lines += [
                "Add the missing index after this statement:",
                "",
                "```suggestion",
                *original_lines,
                block,
                "```",
                "",
                "> ⚠️ `CONCURRENTLY` cannot run inside a transaction block. "
                "Wrap in `op.get_context().autocommit_block()` or split "
                "into a separate non-transactional migration file.",
            ]
        else:
            # Fallback: raw SQL suggestion appended via op.execute()
            lines += [
                "Add the missing index after this statement:",
                "",
                "```suggestion",
                *original_lines,
                f'{indent}op.execute("{fix_sql}")',
                "```",
            ]
        return "\n".join(lines)

    # Additive fix without source context: show SQL block (not committable)
    if fix_sql and fix_type == "additive":
        lines += [
            "Add the missing index in a separate non-transactional migration:",
            "",
            "```sql",
            fix_sql,
            "```",
            "",
            "> ⚠️ `CONCURRENTLY` cannot run inside a transaction block. "
            "Add this in a new migration using `op.get_context().autocommit_block()`.",
        ]
    # Replace fix: swap the bad line with the corrected version
    elif fix_sql and source_line is not None:
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
        # If the action contains a code fence, emit it verbatim — wrapping in
        # a blockquote (`> ...`) breaks code block rendering on GitHub.
        if "```" in suggested_action:
            lines += ["", suggested_action]
        else:
            lines += [f"> {suggested_action}"]

    return "\n".join(lines)


def _make_cross_cutting_body(finding: dict[str, Any]) -> str:
    """Build the markdown body for a cross-cutting inline review comment.

    Anchored to the *cause* line (the change in the diff that will break something
    elsewhere).  Includes a pointer to the affected file and query so the developer
    knows exactly what will degrade.
    """
    severity = finding.get("severity", "INFO")
    detector = finding.get("detector_name", "")
    cause_context = finding.get("cause_context", "")
    description = finding.get("description", "")
    suggested_action = finding.get("suggested_action", "")
    query_source = finding.get("query_source") or {}
    query_file = query_source.get("file", "")
    query_line = query_source.get("line_number")

    icon = _SEV_ICON.get(severity, "ℹ️")
    why = _DETECTOR_WHY.get(detector, "")

    lines = [f"**{icon} `{detector}`**", ""]

    if cause_context:
        lines += [f"> {cause_context}", ""]

    if why:
        lines += [why, ""]
    elif description:
        lines += [description, ""]

    if query_file:
        location = f"`{query_file}`"
        if query_line:
            location += f":L{query_line}"
        lines += [f"**Affected query:** {location}", ""]

    if suggested_action:
        fix_sql = _extract_fix_sql(suggested_action)
        if fix_sql:
            sql_match = _SQL_FROM_ACTION_RE.search(suggested_action)
            prose = suggested_action[: sql_match.start()].strip() if sql_match else ""
            if prose:
                lines += [f"> {prose}", ""]
            lines += ["```sql", fix_sql, "```"]
        else:
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
    diff_path: str | None = None,
) -> None:
    """
    Post a GitHub PR review with inline suggestion comments.

    Issues are grouped by (source_file, detector_name) so that a batch of
    identical findings (e.g. 13 CREATE INDEX without CONCURRENTLY in one
    migration) produces ONE inline comment on the first occurrence, with a
    note listing the other affected lines — instead of N identical comments.

    GitHub limits review comment submissions to 64 comments per review;
    we cap at 50 to stay safe.

    TODO: function-scoped batch suggestion (Tier 3).
    When all occurrences of a detector belong to the same function
    (upgrade vs downgrade), the ideal UX is a single autocommit_block()
    suggestion wrapping all operations using a start_line + line range
    comment. Requires storing function context on each Issue at extraction
    time and using GitHub's multi-line comment API.
    """
    # Group by (source_file, detector) so that a batch of identical issues
    # (e.g. 13 CREATE INDEX without CONCURRENTLY) produces ONE inline comment
    # on the first occurrence instead of N identical comments.
    # Value: sorted list of (line_number, severity, suggested_action)
    # Value: list of (line_number, severity, suggested_action, start_line)
    # (line_number, severity, suggested_action, start_line, fix_type)
    groups: dict[tuple[str, str], list[tuple[int, str, str, int | None, str]]] = (
        defaultdict(list)
    )

    for result in report.get("results", []):
        source_file = result.get("source_file", "")
        line_number = result.get("line_number")
        if not source_file or not line_number:
            continue
        for issue in result.get("issues", []):
            detector = issue.get("detector_name", "")
            severity = issue.get("severity", "INFO")
            suggested_action = issue.get("suggested_action", "")
            fix_type = issue.get("fix_type", "replace")
            groups[(source_file, detector)].append(
                (line_number, severity, suggested_action, None, fix_type)
            )

    for issue in report.get("code_pattern_issues", []):
        source_file = issue.get("source_file") or ""
        line_number = issue.get("line_number")
        if not source_file or not line_number:
            continue
        detector = issue.get("detector_name", "")
        severity = issue.get("severity", "INFO")
        suggested_action = issue.get("suggested_action", "")
        start_line = issue.get("start_line")
        fix_type = issue.get("fix_type", "replace")
        groups[(source_file, detector)].append(
            (line_number, severity, suggested_action, start_line, fix_type)
        )

    for entry in report.get("model_diffs", []):
        source_file = entry.get("file") or ""
        for issue in entry.get("model_issues", []):
            line_number = issue.get("line_number")
            if not source_file or not line_number:
                continue
            detector = issue.get("detector_name", "")
            severity = issue.get("severity", "INFO")
            suggested_action = issue.get("suggested_action", "")
            fix_type = issue.get("fix_type", "replace")
            groups[(source_file, detector)].append(
                (line_number, severity, suggested_action, None, fix_type)
            )

    comments: list[dict[str, Any]] = []

    # ── Cross-cutting findings — anchor to cause line ─────────────────────────
    # These are findings where the *cause* is a line in the diff but the
    # *effect* is in a file not touched by the PR (Type C).  Without this block
    # they only appear in the summary comment; here we post them as inline
    # comments on the migration/model line that introduced the problem.
    for finding in report.get("cross_cutting_findings", []):
        cause_file = finding.get("cause_file")
        cause_line = finding.get("cause_line")
        if not cause_file or not cause_line:
            continue
        body = _make_cross_cutting_body(finding)
        comments.append(
            {
                "path": cause_file,
                "line": cause_line,
                "side": "RIGHT",
                "body": body,
            }
        )
        if len(comments) >= 50:
            break

    for (source_file, detector), occurrences in groups.items():
        occurrences.sort(key=lambda x: x[0])
        first_line, severity, suggested_action, start_line, fix_type = occurrences[0]
        count = len(occurrences)

        source_line = _read_file_line(source_file, first_line)

        # For additive fixes, find the full statement span so the
        # suggestion can reproduce the original lines + append the fix.
        original_lines: list[str] | None = None
        end_line = first_line
        if fix_type == "additive" and source_line is not None:
            span_start, end_line = _find_call_span(source_file, first_line)
            original_lines = _read_file_lines(source_file, span_start, end_line)
            # Override first_line so the comment start covers the full call
            first_line = span_start

        body = _make_suggestion_body(
            detector,
            severity,
            suggested_action,
            source_line,
            fix_type,
            original_lines=original_lines,
        )

        if count > 1:
            preview = occurrences[1:4]
            lines_str = ", ".join(f"L{ln}" for ln, _, _, _, _ in preview)
            if count > 4:
                lines_str += f", …+{count - 4} more"
            body += (
                f"\n\n---\n_**{count} occurrences** in this file "
                f"({lines_str}). Same fix applies to each._"
            )

        comment: dict[str, Any] = {
            "path": source_file,
            "line": end_line,
            "side": "RIGHT",
            "body": body,
        }
        # Multi-line suggestion: cover start_line→line so GitHub replaces
        # the entire bad block (e.g. f-string building + execute() call).
        actual_start = start_line if start_line and start_line < end_line else None
        if actual_start is None and first_line < end_line:
            actual_start = first_line
        if actual_start:
            comment["start_line"] = actual_start
            comment["start_side"] = "RIGHT"
        comments.append(comment)

        if len(comments) >= 50:
            break

    # Filter to lines actually present in the diff — the GitHub PR Review API
    # rejects any comment referencing a line outside diff hunks (HTTP 422).
    if diff_path:
        diff_index = _build_diff_index(diff_path)
        if diff_index:
            filtered: list[dict[str, Any]] = []
            skipped = 0
            for comment in comments:
                path, line = comment["path"], comment["line"]
                if (path, line) not in diff_index:
                    skipped += 1
                    continue
                # start_line must also be in the diff; if not, drop it so the
                # comment becomes a single-line comment at `line`.
                sl = comment.get("start_line")
                if sl and (path, sl) not in diff_index:
                    comment = {
                        k: v
                        for k, v in comment.items()
                        if k not in ("start_line", "start_side")
                    }
                filtered.append(comment)
            if skipped:
                print(
                    f"Skipped {skipped} inline suggestion(s) not in the diff.",
                    file=sys.stderr,
                )
            comments = filtered

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
        if "422" not in str(exc):
            print(f"Warning: could not post inline review: {exc}", file=sys.stderr)
            return
        # Batch rejected (likely a comment references a line not in the diff).
        # Fall back to posting each comment individually so valid ones still appear.
        print(
            f"Warning: batch review rejected ({exc}); retrying comments individually.",
            file=sys.stderr,
        )
        posted = 0
        for comment in comments:
            payload: dict[str, Any] = {
                "commit_id": commit_sha,
                "path": comment["path"],
                "line": comment["line"],
                "side": comment.get("side", "RIGHT"),
                "body": comment["body"],
            }
            if "start_line" in comment:
                payload["start_line"] = comment["start_line"]
                payload["start_side"] = comment.get("start_side", "RIGHT")
            try:
                _github_request(
                    "POST",
                    f"{_GITHUB_API_BASE}/repos/{repo}/pulls/{pr_number}/comments",
                    token,
                    payload=payload,
                )
                posted += 1
            except RuntimeError as inner_exc:
                print(
                    f"Warning: could not post inline comment on "
                    f"{comment['path']}:{comment['line']}: {inner_exc}",
                    file=sys.stderr,
                )
        print(f"Posted {posted}/{len(comments)} inline suggestion(s) individually.")
