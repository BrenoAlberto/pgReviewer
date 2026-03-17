from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx

from pgreviewer.reporting.comment_manager import (
    post_or_update_comment as _post_or_update_comment,
)

_GITHUB_API_BASE = "https://api.github.com"

_OUTCOME_TO_CONCLUSION = {
    "pass": "success",
    "fail": "failure",
    "warning": "neutral",
    "success": "success",
    "failure": "failure",
    "neutral": "neutral",
}


def _headers(token: str, accept: str = "application/vnd.github+json") -> dict[str, str]:
    return {
        "Accept": accept,
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "pgreviewer",
    }


def get_pr_diff(pr_number: int, repo: str, token: str) -> str:
    url = f"{_GITHUB_API_BASE}/repos/{repo}/pulls/{pr_number}"
    response = httpx.get(
        url,
        headers=_headers(token, accept="application/vnd.github.v3.diff"),
        follow_redirects=True,
    )
    response.raise_for_status()
    return response.text


def post_or_update_comment(pr_number: int, repo: str, token: str, body: str) -> None:
    _post_or_update_comment(pr_number=pr_number, repo=repo, token=token, body=body)


def set_check_status(
    sha: str,
    repo: str,
    token: str,
    outcome: str,
    report_url: str | None = None,
) -> None:
    conclusion = _OUTCOME_TO_CONCLUSION.get(outcome, "neutral")
    payload: dict[str, Any] = {
        "name": "pgreviewer",
        "head_sha": sha,
        "status": "completed",
        "conclusion": conclusion,
        "completed_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "output": {
            "title": f"pgreviewer: {outcome}",
            "summary": f"Query analysis result: **{outcome}**",
        },
    }
    if report_url:
        payload["details_url"] = report_url

    url = f"{_GITHUB_API_BASE}/repos/{repo}/check-runs"
    response = httpx.post(url, headers=_headers(token), json=payload)
    response.raise_for_status()
