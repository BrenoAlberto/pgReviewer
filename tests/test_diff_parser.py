from pathlib import Path

import pytest

from pgreviewer.parsing.diff_parser import ChangedFile, parse_diff

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "diff"


def _load_patch(name: str) -> str:
    return (FIXTURE_DIR / name).read_text()


def test_parse_diff_returns_list_of_changed_files():
    diff_str = _load_patch("sample.patch")
    result = parse_diff(diff_str)

    assert isinstance(result, list)
    assert len(result) == 2
    assert all(isinstance(f, ChangedFile) for f in result)


def test_new_file_is_detected():
    diff_str = _load_patch("sample.patch")
    result = parse_diff(diff_str)

    new_file = next(f for f in result if f.path == "migrations/0001_init.sql")
    assert new_file.is_new_file is True


def test_modified_file_is_not_new():
    diff_str = _load_patch("sample.patch")
    result = parse_diff(diff_str)

    modified = next(f for f in result if f.path == "migrations/0002_orders.sql")
    assert modified.is_new_file is False


def test_only_added_lines_are_returned():
    """Deleted lines must not appear in added_lines."""
    diff_str = _load_patch("sample.patch")
    result = parse_diff(diff_str)

    modified = next(f for f in result if f.path == "migrations/0002_orders.sql")

    for line in modified.added_lines:
        assert not line.startswith("-"), (
            f"Deleted line leaked into added_lines: {line!r}"
        )


def test_added_lines_have_leading_plus_stripped():
    """Lines must not start with the '+' diff marker."""
    diff_str = _load_patch("sample.patch")
    result = parse_diff(diff_str)

    for changed_file in result:
        for line in changed_file.added_lines:
            assert not line.startswith("+"), f"Leading '+' not stripped: {line!r}"


def test_new_file_captures_all_added_lines():
    diff_str = _load_patch("sample.patch")
    result = parse_diff(diff_str)

    init_file = next(f for f in result if f.path == "migrations/0001_init.sql")
    assert len(init_file.added_lines) == 6
    assert "CREATE TABLE users (" in init_file.added_lines
    assert "CREATE INDEX idx_users_email ON users (email);" in init_file.added_lines


def test_modified_file_added_lines_content():
    diff_str = _load_patch("sample.patch")
    result = parse_diff(diff_str)

    modified = next(f for f in result if f.path == "migrations/0002_orders.sql")
    assert any("NOT NULL" in line for line in modified.added_lines)
    assert any("updated_at" in line for line in modified.added_lines)


def test_added_line_numbers_are_populated():
    diff_str = _load_patch("sample.patch")
    result = parse_diff(diff_str)

    for changed_file in result:
        assert len(changed_file.added_lines) == len(changed_file.added_line_numbers)
        for num in changed_file.added_line_numbers:
            assert isinstance(num, int)
            assert num >= 1


def test_added_line_numbers_for_new_file():
    diff_str = _load_patch("sample.patch")
    result = parse_diff(diff_str)

    init_file = next(f for f in result if f.path == "migrations/0001_init.sql")
    assert init_file.added_line_numbers == list(range(1, 7))


def test_parse_diff_empty_string():
    result = parse_diff("")
    assert result == []


def test_parse_diff_inline():
    diff_str = (
        "diff --git a/query.sql b/query.sql\n"
        "--- a/query.sql\n"
        "+++ b/query.sql\n"
        "@@ -1,2 +1,2 @@\n"
        " SELECT id\n"
        "-FROM orders;\n"
        "+FROM orders WHERE status = 'active';\n"
    )
    result = parse_diff(diff_str)

    assert len(result) == 1
    assert result[0].path == "query.sql"
    assert result[0].added_lines == ["FROM orders WHERE status = 'active';"]
    assert result[0].added_line_numbers == [2]
    assert result[0].is_new_file is False


@pytest.mark.parametrize(
    "line",
    [
        "    id SERIAL PRIMARY KEY,",
        "    email TEXT NOT NULL UNIQUE,",
        "    created_at TIMESTAMPTZ DEFAULT now()",
        "CREATE INDEX idx_users_email ON users (email);",
    ],
)
def test_new_file_specific_lines_present(line):
    diff_str = _load_patch("sample.patch")
    result = parse_diff(diff_str)

    init_file = next(f for f in result if f.path == "migrations/0001_init.sql")
    assert line in init_file.added_lines
