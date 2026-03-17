from unittest.mock import MagicMock, patch

import httpx
import pytest

from pgreviewer.ci.github_integration import (
    get_pr_diff,
    post_or_update_comment,
    set_check_status,
)


def _mock_response(
    status_code: int = 200,
    text: str = "",
    json_data: dict | None = None,
) -> MagicMock:
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    response.text = text
    if json_data is not None:
        response.json.return_value = json_data
    response.raise_for_status = MagicMock()
    return response


def test_get_pr_diff_returns_diff_text() -> None:
    diff_text = "diff --git a/foo.py b/foo.py\n+new line\n"
    response = _mock_response(text=diff_text)

    with patch(
        "pgreviewer.ci.github_integration.httpx.get", return_value=response
    ) as mock_get:
        result = get_pr_diff(42, "owner/repo", "token")

    assert result == diff_text
    mock_get.assert_called_once_with(
        "https://api.github.com/repos/owner/repo/pulls/42",
        headers={
            "Accept": "application/vnd.github.v3.diff",
            "Authorization": "Bearer token",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "pgreviewer",
        },
        follow_redirects=True,
    )
    response.raise_for_status.assert_called_once()


def test_get_pr_diff_raises_on_http_error() -> None:
    response = _mock_response(status_code=404)
    response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Not Found", request=MagicMock(), response=response
    )

    with (
        patch("pgreviewer.ci.github_integration.httpx.get", return_value=response),
        pytest.raises(httpx.HTTPStatusError),
    ):
        get_pr_diff(99, "owner/repo", "token")


def test_post_or_update_comment_delegates_to_comment_manager() -> None:
    with patch("pgreviewer.ci.github_integration._post_or_update_comment") as mock_post:
        post_or_update_comment(7, "owner/repo", "token", "body text")

    mock_post.assert_called_once_with(
        pr_number=7, repo="owner/repo", token="token", body="body text"
    )


@pytest.mark.parametrize(
    ("outcome", "expected_conclusion"),
    [
        ("pass", "success"),
        ("fail", "failure"),
        ("warning", "neutral"),
        ("unknown", "neutral"),
    ],
)
def test_set_check_status_maps_outcome_to_conclusion(
    outcome: str, expected_conclusion: str
) -> None:
    response = _mock_response(status_code=201)

    with patch(
        "pgreviewer.ci.github_integration.httpx.post", return_value=response
    ) as mock_post:
        set_check_status("abc123", "owner/repo", "token", outcome)

    mock_post.assert_called_once()
    _, kwargs = mock_post.call_args
    payload = kwargs["json"]
    assert payload["conclusion"] == expected_conclusion
    assert payload["head_sha"] == "abc123"
    assert payload["status"] == "completed"
    assert payload["name"] == "pgreviewer"
    response.raise_for_status.assert_called_once()


def test_set_check_status_includes_details_url_when_report_url_provided() -> None:
    response = _mock_response(status_code=201)

    with patch(
        "pgreviewer.ci.github_integration.httpx.post", return_value=response
    ) as mock_post:
        set_check_status(
            "abc123",
            "owner/repo",
            "token",
            "pass",
            report_url="https://example.com/report",
        )

    _, kwargs = mock_post.call_args
    assert kwargs["json"]["details_url"] == "https://example.com/report"


def test_set_check_status_omits_details_url_when_none() -> None:
    response = _mock_response(status_code=201)

    with patch(
        "pgreviewer.ci.github_integration.httpx.post", return_value=response
    ) as mock_post:
        set_check_status("abc123", "owner/repo", "token", "pass", report_url=None)

    _, kwargs = mock_post.call_args
    assert "details_url" not in kwargs["json"]


def test_set_check_status_posts_to_correct_url() -> None:
    response = _mock_response(status_code=201)

    with patch(
        "pgreviewer.ci.github_integration.httpx.post", return_value=response
    ) as mock_post:
        set_check_status("sha1", "myorg/myrepo", "token", "pass")

    url = mock_post.call_args[0][0]
    assert url == "https://api.github.com/repos/myorg/myrepo/check-runs"
