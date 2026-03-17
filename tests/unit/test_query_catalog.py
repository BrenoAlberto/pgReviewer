from __future__ import annotations

import time

from pgreviewer.analysis.query_catalog import build_catalog


def _write_repository_file(path, *, include_list_method: bool = False) -> None:
    extra_method = (
        "\n    def list_all(self):\n"
        '        return self.session.execute("SELECT id FROM users")\n'
        if include_list_method
        else ""
    )
    path.write_text(
        "class UserRepository:\n"
        "    def __init__(self, session):\n"
        "        self.session = session\n\n"
        "    def get_by_id(self, user_id):\n"
        '        return self.session.execute("SELECT * FROM users WHERE id = :id")\n'
        f"{extra_method}",
        encoding="utf-8",
    )


def test_build_catalog_discovers_query_functions_and_writes_cache(tmp_path) -> None:
    src = tmp_path / "src"
    src.mkdir()
    repo_file = src / "repository.py"
    _write_repository_file(repo_file)

    catalog = build_catalog(src, force_rebuild=True)

    assert "repository.UserRepository.get_by_id" in catalog.functions
    query_info = catalog.functions["repository.UserRepository.get_by_id"]
    assert query_info.file == "repository.py"
    assert query_info.method_name == "execute"
    assert query_info.query_text_if_available == "SELECT * FROM users WHERE id = :id"

    assert (src / ".pgreviewer/query_catalog.json").is_file()


def test_build_catalog_rebuilds_when_cache_is_stale(tmp_path) -> None:
    src = tmp_path / "src"
    src.mkdir()
    repo_file = src / "repository.py"
    _write_repository_file(repo_file)

    first_catalog = build_catalog(src, force_rebuild=True)
    assert "repository.UserRepository.list_all" not in first_catalog.functions
    cache_file = src / ".pgreviewer/query_catalog.json"
    first_cache_mtime = cache_file.stat().st_mtime

    time.sleep(1.1)
    _write_repository_file(repo_file, include_list_method=True)

    second_catalog = build_catalog(src)

    assert "repository.UserRepository.list_all" in second_catalog.functions
    assert cache_file.stat().st_mtime > first_cache_mtime
