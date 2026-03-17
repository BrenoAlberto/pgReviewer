from __future__ import annotations

from pathlib import Path

import pytest

from pgreviewer.analysis.model_issue_detectors import run_model_issue_detectors
from pgreviewer.parsing.model_differ import diff_models
from pgreviewer.parsing.sqlalchemy_analyzer import ModelDefinition, analyze_model_file

_FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "models"


def _model_from_fixture(filename: str) -> ModelDefinition:
    models = analyze_model_file(_FIXTURES_DIR / filename)
    assert len(models) == 1
    return models[0]


def test_sqlalchemy_analyzer_missing_fk_index_fixture():
    model = _model_from_fixture("missing_fk_index.py")

    assert model.class_name == "MissingFkIndexModel"
    assert model.table_name == "orders"
    assert {c.name for c in model.columns} == {"id", "user_id", "status"}

    user_id = next(c for c in model.columns if c.name == "user_id")
    assert user_id.index is False
    assert user_id.nullable is False

    assert len(model.foreign_keys) == 1
    assert model.foreign_keys[0].column_name == "user_id"
    assert model.foreign_keys[0].target == "users.id"


def test_sqlalchemy_analyzer_good_model_fixture():
    model = _model_from_fixture("good_model.py")

    assert model.class_name == "GoodModel"
    assert model.table_name == "orders"
    assert len(model.foreign_keys) == 1
    assert model.foreign_keys[0].target == "users.id"

    by_name = {c.name: c for c in model.columns}
    assert by_name["user_id"].index is True
    assert by_name["status"].index is True
    assert by_name["created_at"].index is True


def test_sqlalchemy_analyzer_removed_index_fixture():
    models = analyze_model_file(_FIXTURES_DIR / "removed_index.py")
    by_name = {m.class_name: m for m in models}

    assert len(models) == 2
    assert len(by_name["RemovedIndexBefore"].indexes) == 1
    assert by_name["RemovedIndexBefore"].indexes[0].name == "ix_orders_status"
    assert by_name["RemovedIndexBefore"].indexes[0].columns == ["status"]
    assert by_name["RemovedIndexAfter"].indexes == []


def test_sqlalchemy_analyzer_orm_queries_fixture_has_no_models():
    assert analyze_model_file(_FIXTURES_DIR / "orm_queries.py") == []


def test_model_differ_removed_index_fixture_pair():
    models = analyze_model_file(_FIXTURES_DIR / "removed_index.py")
    by_name = {m.class_name: m for m in models}

    diff = diff_models(by_name["RemovedIndexBefore"], by_name["RemovedIndexAfter"])

    assert diff.added_indexes == []
    assert len(diff.removed_indexes) == 1
    assert diff.removed_indexes[0].name == "ix_orders_status"
    assert diff.has_changes is True


@pytest.mark.parametrize(
    ("fixture_name", "expected_issue_count"),
    [
        ("missing_fk_index.py", 1),
        ("good_model.py", 0),
        ("removed_index.py", 1),
        ("orm_queries.py", 0),
    ],
)
def test_model_issue_detectors_fixture_issue_counts(
    fixture_name: str, expected_issue_count: int
):
    if fixture_name == "removed_index.py":
        models = analyze_model_file(_FIXTURES_DIR / fixture_name)
        by_name = {m.class_name: m for m in models}
        diff = diff_models(by_name["RemovedIndexBefore"], by_name["RemovedIndexAfter"])
    else:
        models = analyze_model_file(_FIXTURES_DIR / fixture_name)
        if not models:
            issues = []
            assert len(issues) == expected_issue_count
            return

        after = models[0]
        before = ModelDefinition(
            class_name=after.class_name,
            table_name=after.table_name,
        )
        diff = diff_models(before, after)

    issues = run_model_issue_detectors(diff)
    assert len(issues) == expected_issue_count
