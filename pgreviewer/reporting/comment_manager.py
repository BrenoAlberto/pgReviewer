from __future__ import annotations

import json
import re
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pgreviewer.reporting.pr_comment import REPORT_SIGNATURE

_GITHUB_API_BASE = "https://api.github.com"
_NEXT_LINK_RE = re.compile(r'<([^>]+)>;\s*rel="next"')


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
