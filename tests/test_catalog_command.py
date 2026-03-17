from typer.testing import CliRunner

from pgreviewer.cli.main import app


def test_catalog_build_and_show(tmp_path) -> None:
    repo_file = tmp_path / "repository.py"
    repo_file.write_text(
        "class UserRepository:\n"
        "    def __init__(self, session):\n"
        "        self.session = session\n\n"
        "    def get_by_id(self, user_id):\n"
        '        return self.session.execute("SELECT * FROM users WHERE id = :id")\n',
        encoding="utf-8",
    )

    runner = CliRunner()
    build_result = runner.invoke(
        app, ["catalog", "build", "--project-root", str(tmp_path)]
    )
    assert build_result.exit_code == 0
    assert "Catalog built with 1 query function(s)" in build_result.stdout

    show_result = runner.invoke(
        app, ["catalog", "show", "--project-root", str(tmp_path)]
    )
    assert show_result.exit_code == 0
    assert "Query Catalog" in show_result.stdout
    assert "repository.py" in show_result.stdout
