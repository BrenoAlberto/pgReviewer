from pgreviewer.config import Settings


def test_trigger_paths_accepts_github_action_input_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://postgres:postgres@localhost/db")
    monkeypatch.setenv("INPUT_TRIGGER_PATHS", "migrations/**,custom/sql/**")

    cfg = Settings()

    assert cfg.TRIGGER_PATHS == ["migrations/**", "custom/sql/**"]
