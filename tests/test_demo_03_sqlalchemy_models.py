from pathlib import Path

from pgreviewer.config import load_pgreviewer_config
from pgreviewer.parsing.file_classifier import FileType, classify_file
from pgreviewer.parsing.sqlalchemy_analyzer import analyze_model_file

REPO_ROOT = Path(__file__).resolve().parent.parent
DEMO_ROOT = REPO_ROOT / "demos" / "03-sqlalchemy-models"


def test_demo_03_models_contract() -> None:
    model_path = DEMO_ROOT / "models.py"
    source = model_path.read_text(encoding="utf-8")

    assert classify_file("models.py", source) == FileType.PYTHON_WITH_SQL

    models = analyze_model_file(model_path)
    by_name = {model.class_name: model for model in models}

    event = by_name["Event"]
    account_fk = next(fk for fk in event.foreign_keys if fk.column_name == "account_id")
    assert account_fk.target == "accounts.id"

    account_id_col = next(col for col in event.columns if col.name == "account_id")
    assert account_id_col.index is False

    account_rel = next(rel for rel in event.relationships if rel.name == "account")
    assert account_rel.back_populates == "events"

    assert "UniqueConstraint" in source


def test_demo_03_readme_and_config_contract() -> None:
    config = load_pgreviewer_config(DEMO_ROOT / ".pgreviewer.yml")
    readme = (DEMO_ROOT / "README.md").read_text(encoding="utf-8")

    assert config.rules["missing_fk_index"].enabled is True
    assert config.rules["missing_fk_index"].severity == "warning"

    assert "pgr diff" in readme
    assert "without a live database connection" in readme
    assert "missing_fk_index" in readme
