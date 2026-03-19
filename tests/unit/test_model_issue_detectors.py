from __future__ import annotations

from pathlib import Path

from pgreviewer.analysis.model_issue_detectors import (
    detect_duplicate_pk_index,
    detect_large_text_without_constraint,
    detect_missing_fk_index,
    detect_removed_index,
    run_model_issue_detectors,
)
from pgreviewer.core.models import SchemaInfo
from pgreviewer.parsing.model_differ import diff_models
from pgreviewer.parsing.sqlalchemy_analyzer import ModelDefinition, analyze_model_file

_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "models"


def _empty_before(class_name: str, table_name: str) -> ModelDefinition:
    return ModelDefinition(class_name=class_name, table_name=table_name)


# ── detect_missing_fk_index ───────────────────────────────────────────────────


def test_missing_fk_index_cause_fields_populated():
    models = analyze_model_file(_FIXTURES / "missing_fk_index.py")
    assert models, "fixture must contain at least one model"
    after = models[0]
    before = _empty_before(after.class_name, after.table_name)
    diff = diff_models(before, after)

    issues = detect_missing_fk_index(diff, source_file="app/models.py")

    assert issues, "expect at least one missing_fk_index issue"
    for issue in issues:
        assert issue.cause_file == "app/models.py"
        assert issue.cause_line is not None and issue.cause_line > 0
        assert issue.cause_context is not None
        assert "added without index" in issue.cause_context


def test_missing_fk_index_no_source_file():
    models = analyze_model_file(_FIXTURES / "missing_fk_index.py")
    after = models[0]
    before = _empty_before(after.class_name, after.table_name)
    diff = diff_models(before, after)

    issues = detect_missing_fk_index(diff)  # no source_file

    for issue in issues:
        assert issue.cause_file is None


# ── detect_removed_index ──────────────────────────────────────────────────────


def test_removed_index_cause_fields_populated():
    models = analyze_model_file(_FIXTURES / "removed_index.py")
    by_name = {m.class_name: m for m in models}
    before = by_name["RemovedIndexBefore"]
    after = by_name["RemovedIndexAfter"]
    diff = diff_models(before, after)

    issues = detect_removed_index(diff, SchemaInfo(), source_file="app/models.py")

    assert issues, "expect at least one removed_index issue"
    for issue in issues:
        assert issue.cause_file == "app/models.py"
        # cause_line is intentionally None for removed items (line is from old file)
        assert issue.cause_line is None
        assert issue.cause_context is not None
        assert "removed here" in issue.cause_context


# ── detect_large_text_without_constraint ──────────────────────────────────────


def test_large_text_without_constraint_cause_fields():
    from pgreviewer.parsing.sqlalchemy_analyzer import ColumnDef

    after = ModelDefinition(class_name="Foo", table_name="foo")
    after.columns.append(ColumnDef(name="bio", col_type="Text", line=10))
    before = _empty_before("Foo", "foo")
    diff = diff_models(before, after)

    issues = detect_large_text_without_constraint(diff, source_file="app/models.py")

    assert issues
    assert issues[0].cause_file == "app/models.py"
    assert issues[0].cause_line == 10
    assert "bio" in issues[0].cause_context


# ── detect_duplicate_pk_index ─────────────────────────────────────────────────


def test_duplicate_pk_index_cause_fields():
    from pgreviewer.parsing.sqlalchemy_analyzer import ColumnDef, IndexDef

    after = ModelDefinition(class_name="Bar", table_name="bar")
    after.columns.append(ColumnDef(name="id", col_type="Integer", primary_key=True))
    after.indexes.append(IndexDef(name="ix_bar_id", columns=["id"], line=15))
    before = _empty_before("Bar", "bar")
    diff = diff_models(before, after)

    issues = detect_duplicate_pk_index(diff, source_file="app/models.py")

    assert issues
    assert issues[0].cause_file == "app/models.py"
    assert issues[0].cause_line == 15
    assert "ix_bar_id" in issues[0].cause_context


# ── run_model_issue_detectors wrapper ─────────────────────────────────────────


def test_run_model_issue_detectors_passes_source_file():
    models = analyze_model_file(_FIXTURES / "missing_fk_index.py")
    after = models[0]
    before = _empty_before(after.class_name, after.table_name)
    diff = diff_models(before, after)

    issues = run_model_issue_detectors(diff, source_file="migrations/0001_initial.py")

    fk_issues = [i for i in issues if i.detector_name == "missing_fk_index"]
    assert fk_issues
    for issue in fk_issues:
        assert issue.cause_file == "migrations/0001_initial.py"
