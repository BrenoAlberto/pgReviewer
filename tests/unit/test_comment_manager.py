from unittest.mock import patch

from pgreviewer.reporting.comment_manager import (
    find_existing_comment,
    post_or_update_comment,
)
from pgreviewer.reporting.pr_comment import REPORT_SIGNATURE


def test_find_existing_comment_returns_latest_signature_comment_id() -> None:
    responses = [
        (
            [
                {"id": 10, "body": "unrelated"},
                {"id": 11, "body": f"{REPORT_SIGNATURE}\nold"},
            ],
            {"Link": '<https://api.github.com/next>; rel="next"'},
        ),
        (
            [
                {"id": 22, "body": f"{REPORT_SIGNATURE}\nnew"},
            ],
            {},
        ),
    ]

    with patch(
        "pgreviewer.reporting.comment_manager._github_request", side_effect=responses
    ) as request:
        comment_id = find_existing_comment(123, "owner/repo", "token")

    assert comment_id == 22
    assert request.call_count == 2


def test_find_existing_comment_returns_none_when_signature_not_found() -> None:
    with patch(
        "pgreviewer.reporting.comment_manager._github_request",
        return_value=([{"id": 10, "body": "hello"}], {}),
    ):
        comment_id = find_existing_comment(123, "owner/repo", "token")

    assert comment_id is None


def test_post_or_update_comment_posts_when_no_existing_comment() -> None:
    with (
        patch(
            "pgreviewer.reporting.comment_manager.find_existing_comment",
            return_value=None,
        ),
        patch("pgreviewer.reporting.comment_manager._github_request") as request,
    ):
        post_or_update_comment(7, "owner/repo", "token", "comment body")

    request.assert_called_once_with(
        "POST",
        "https://api.github.com/repos/owner/repo/issues/7/comments",
        "token",
        payload={"body": "comment body"},
    )


def test_post_or_update_comment_updates_when_existing_comment_found() -> None:
    with (
        patch(
            "pgreviewer.reporting.comment_manager.find_existing_comment",
            return_value=99,
        ),
        patch("pgreviewer.reporting.comment_manager._github_request") as request,
    ):
        post_or_update_comment(7, "owner/repo", "token", "comment body")

    request.assert_called_once_with(
        "PATCH",
        "https://api.github.com/repos/owner/repo/issues/comments/99",
        "token",
        payload={"body": "comment body"},
    )
