"""Unit tests for pgreviewer.parsing.model_differ.

Covers:
- diff_models: new column identified
- diff_models: removed index identified
- diff_models: added/removed relationships
- diff_models: empty diff when models are identical
- diff_models: new model class (empty before) shows all fields as added
- ModelDiff.has_changes property
- _get_file_at_ref: happy path and error paths (mocked subprocess)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from pgreviewer.parsing.model_differ import diff_models
from pgreviewer.parsing.sqlalchemy_analyzer import (
    ColumnDef,
    IndexDef,
    ModelDefinition,
    RelDef,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _model(
    class_name: str = "MyModel",
    table_name: str = "my_table",
    columns: list[ColumnDef] | None = None,
    indexes: list[IndexDef] | None = None,
    relationships: list[RelDef] | None = None,
) -> ModelDefinition:
    return ModelDefinition(
        class_name=class_name,
        table_name=table_name,
        columns=columns or [],
        indexes=indexes or [],
        relationships=relationships or [],
    )


def _col(name: str, col_type: str = "String") -> ColumnDef:
    return ColumnDef(name=name, col_type=col_type)


def _idx(name: str | None, columns: list[str], is_unique: bool = False) -> IndexDef:
    return IndexDef(name=name, columns=columns, is_unique=is_unique)


def _rel(name: str, target: str) -> RelDef:
    return RelDef(name=name, target_model=target)


# ===========================================================================
# diff_models – column changes
# ===========================================================================


class TestDiffModelsColumns:
    """diff_models correctly identifies added and removed columns."""

    def test_new_status_column_detected(self):
        """Adding a 'status' column should appear in added_columns."""
        before = _model(columns=[_col("id", "Integer"), _col("name", "String")])
        after = _model(
            columns=[
                _col("id", "Integer"),
                _col("name", "String"),
                _col("status", "String"),
            ]
        )
        result = diff_models(before, after)
        assert len(result.added_columns) == 1
        assert result.added_columns[0].name == "status"
        assert result.added_columns[0].col_type == "String"
        assert result.removed_columns == []

    def test_removed_column_detected(self):
        """A column present in before but absent in after is removed."""
        before = _model(columns=[_col("id", "Integer"), _col("legacy_col", "String")])
        after = _model(columns=[_col("id", "Integer")])
        result = diff_models(before, after)
        assert result.added_columns == []
        assert len(result.removed_columns) == 1
        assert result.removed_columns[0].name == "legacy_col"

    def test_simultaneous_add_and_remove(self):
        """One column added and one removed are both captured."""
        before = _model(columns=[_col("id"), _col("old_col")])
        after = _model(columns=[_col("id"), _col("new_col")])
        result = diff_models(before, after)
        assert [c.name for c in result.added_columns] == ["new_col"]
        assert [c.name for c in result.removed_columns] == ["old_col"]

    def test_no_column_changes(self):
        """Identical column sets produce empty added/removed lists."""
        cols = [_col("id", "Integer"), _col("name", "String")]
        before = _model(columns=cols[:])
        after = _model(columns=cols[:])
        result = diff_models(before, after)
        assert result.added_columns == []
        assert result.removed_columns == []


# ===========================================================================
# diff_models – index changes
# ===========================================================================


class TestDiffModelsIndexes:
    """diff_models correctly identifies added and removed named indexes."""

    def test_removed_legacy_id_index(self):
        """A removed index appears in removed_indexes."""
        legacy_idx = _idx("ix_legacy_id", ["legacy_id"])
        before = _model(indexes=[legacy_idx])
        after = _model(indexes=[])
        result = diff_models(before, after)
        assert result.added_indexes == []
        assert len(result.removed_indexes) == 1
        assert result.removed_indexes[0].name == "ix_legacy_id"
        assert result.removed_indexes[0].columns == ["legacy_id"]

    def test_new_index_detected(self):
        """A new index appears in added_indexes."""
        new_idx = _idx("ix_status", ["status"])
        before = _model(indexes=[])
        after = _model(indexes=[new_idx])
        result = diff_models(before, after)
        assert len(result.added_indexes) == 1
        assert result.added_indexes[0].name == "ix_status"
        assert result.removed_indexes == []

    def test_unchanged_indexes_not_reported(self):
        """An index that exists in both versions does not appear in diff."""
        idx = _idx("ix_user_id", ["user_id"])
        before = _model(indexes=[idx])
        after = _model(indexes=[idx])
        result = diff_models(before, after)
        assert result.added_indexes == []
        assert result.removed_indexes == []


# ===========================================================================
# diff_models – relationship changes
# ===========================================================================


class TestDiffModelsRelationships:
    """diff_models correctly identifies added and removed relationships."""

    def test_new_relationship_detected(self):
        """A relationship added in after appears in added_relationships."""
        before = _model(relationships=[])
        after = _model(relationships=[_rel("orders", "Order")])
        result = diff_models(before, after)
        assert len(result.added_relationships) == 1
        assert result.added_relationships[0].name == "orders"
        assert result.removed_relationships == []

    def test_removed_relationship_detected(self):
        """A relationship present in before but absent in after is removed."""
        before = _model(relationships=[_rel("items", "Item")])
        after = _model(relationships=[])
        result = diff_models(before, after)
        assert result.added_relationships == []
        assert len(result.removed_relationships) == 1
        assert result.removed_relationships[0].name == "items"

    def test_unchanged_relationships_not_reported(self):
        """A relationship unchanged between versions does not appear in diff."""
        rel = _rel("user", "User")
        before = _model(relationships=[rel])
        after = _model(relationships=[rel])
        result = diff_models(before, after)
        assert result.added_relationships == []
        assert result.removed_relationships == []


# ===========================================================================
# ModelDiff.has_changes
# ===========================================================================


class TestModelDiffHasChanges:
    def test_empty_diff_has_no_changes(self):
        before = _model(columns=[_col("id")])
        after = _model(columns=[_col("id")])
        result = diff_models(before, after)
        assert result.has_changes is False

    def test_diff_with_added_column_has_changes(self):
        before = _model()
        after = _model(columns=[_col("status")])
        result = diff_models(before, after)
        assert result.has_changes is True

    def test_diff_with_removed_index_has_changes(self):
        before = _model(indexes=[_idx("ix_a", ["a"])])
        after = _model()
        result = diff_models(before, after)
        assert result.has_changes is True


# ===========================================================================
# diff_models – class_name / table_name propagated from after
# ===========================================================================


class TestDiffModelsMetadata:
    def test_class_name_from_after(self):
        before = _model(class_name="OldName", table_name="old_table")
        after = _model(class_name="NewName", table_name="new_table")
        result = diff_models(before, after)
        assert result.class_name == "NewName"
        assert result.table_name == "new_table"


# ===========================================================================
# New model (empty before) – all fields shown as added
# ===========================================================================


class TestNewModelAllAdded:
    """When before is an empty ModelDefinition, everything in after is 'added'."""

    def test_all_columns_added(self):
        before = _model()
        after = _model(columns=[_col("id", "Integer"), _col("name", "String")])
        result = diff_models(before, after)
        assert len(result.added_columns) == 2
        assert {c.name for c in result.added_columns} == {"id", "name"}

    def test_all_indexes_added(self):
        before = _model()
        after = _model(indexes=[_idx("ix_name", ["name"])])
        result = diff_models(before, after)
        assert len(result.added_indexes) == 1

    def test_all_relationships_added(self):
        before = _model()
        after = _model(relationships=[_rel("orders", "Order")])
        result = diff_models(before, after)
        assert len(result.added_relationships) == 1


# ===========================================================================
# Scenario from the issue: new status column + removed legacy_id index
# ===========================================================================


class TestIssueScenario:
    """Scenario: new status column added and legacy_id index removed.

    Before: has legacy_id column and ix_legacy_id index.
    After:  adds status column, removes legacy_id index.
    """

    BEFORE = _model(
        class_name="Order",
        table_name="orders",
        columns=[
            _col("id", "Integer"),
            _col("legacy_id", "Integer"),
        ],
        indexes=[
            _idx("ix_legacy_id", ["legacy_id"]),
        ],
    )

    AFTER = _model(
        class_name="Order",
        table_name="orders",
        columns=[
            _col("id", "Integer"),
            _col("status", "String"),
        ],
        indexes=[],
    )

    def setup_method(self):
        self.result = diff_models(self.BEFORE, self.AFTER)

    def test_status_column_added(self):
        assert len(self.result.added_columns) == 1
        assert self.result.added_columns[0].name == "status"

    def test_legacy_id_column_removed(self):
        assert len(self.result.removed_columns) == 1
        assert self.result.removed_columns[0].name == "legacy_id"

    def test_legacy_id_index_removed(self):
        assert len(self.result.removed_indexes) == 1
        assert self.result.removed_indexes[0].name == "ix_legacy_id"

    def test_no_added_indexes(self):
        assert self.result.added_indexes == []

    def test_has_changes(self):
        assert self.result.has_changes is True


# ===========================================================================
# _get_file_at_ref – unit tests via mocked subprocess
# ===========================================================================


def _make_proc(stdout: str = "", returncode: int = 0, stderr: str = "") -> MagicMock:
    proc = MagicMock()
    proc.stdout = stdout
    proc.returncode = returncode
    proc.stderr = stderr
    return proc


class TestGetFileAtRef:
    """Tests for _get_file_at_ref in the diff command module."""

    def _import(self):
        from pgreviewer.cli.commands.diff import _get_file_at_ref

        return _get_file_at_ref

    def test_happy_path_returns_content(self):
        content = "class User(Base):\n    __tablename__ = 'users'\n"
        fn = self._import()
        with patch(
            "subprocess.run", return_value=_make_proc(stdout=content)
        ) as mock_run:
            result = fn("main", "app/models.py")
        mock_run.assert_called_once_with(
            ["git", "show", "main:app/models.py"],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result == content

    def test_nonzero_returncode_returns_none(self):
        fn = self._import()
        with patch("subprocess.run", return_value=_make_proc(returncode=128)):
            result = fn("HEAD~1", "missing_file.py")
        assert result is None

    def test_git_not_installed_returns_none(self):
        fn = self._import()
        with patch("subprocess.run", side_effect=FileNotFoundError):
            result = fn("main", "models.py")
        assert result is None

    def test_uses_correct_git_show_format(self):
        """The command must be ``git show <ref>:<path>`` (colon separator)."""
        fn = self._import()
        with patch("subprocess.run", return_value=_make_proc(stdout="x")) as mock_run:
            fn("abc123", "path/to/file.py")
        args = mock_run.call_args[0][0]
        assert args == ["git", "show", "abc123:path/to/file.py"]


# ===========================================================================
# _collect_model_diffs – integration-level unit test (all mocked)
# ===========================================================================


class TestCollectModelDiffs:
    """Tests for _collect_model_diffs using mocked git and inline source."""

    BEFORE_SOURCE = """\
class Widget(Base):
    __tablename__ = "widgets"
    id = Column(Integer, primary_key=True)
"""

    AFTER_SOURCE = """\
class Widget(Base):
    __tablename__ = "widgets"
    id = Column(Integer, primary_key=True)
    color = Column(String(50), nullable=False)
"""

    def test_detects_added_column_via_git(self):
        from pgreviewer.cli.commands.diff import _collect_model_diffs

        model_diff_results: list[dict] = []
        with patch(
            "pgreviewer.cli.commands.diff._get_file_at_ref",
            return_value=self.BEFORE_SOURCE,
        ):
            _collect_model_diffs(
                "app/models.py",
                self.AFTER_SOURCE,
                "main",
                model_diff_results,
            )

        assert len(model_diff_results) == 1
        entry = model_diff_results[0]
        assert entry["file"] == "app/models.py"
        assert len(entry["diffs"]) == 1
        diff = entry["diffs"][0]
        assert diff.class_name == "Widget"
        assert len(diff.added_columns) == 1
        assert diff.added_columns[0].name == "color"

    def test_no_changes_produces_no_entry(self):
        from pgreviewer.cli.commands.diff import _collect_model_diffs

        model_diff_results: list[dict] = []
        with patch(
            "pgreviewer.cli.commands.diff._get_file_at_ref",
            return_value=self.AFTER_SOURCE,  # same content = no diff
        ):
            _collect_model_diffs(
                "app/models.py",
                self.AFTER_SOURCE,
                "main",
                model_diff_results,
            )

        assert model_diff_results == []

    def test_no_before_ref_treats_model_as_new(self):
        """When before_ref is None, all columns in after are treated as added."""
        from pgreviewer.cli.commands.diff import _collect_model_diffs

        model_diff_results: list[dict] = []
        _collect_model_diffs(
            "app/models.py",
            self.AFTER_SOURCE,
            None,  # no git ref
            model_diff_results,
        )

        assert len(model_diff_results) == 1
        diff = model_diff_results[0]["diffs"][0]
        # Both id and color should be "added" (entire new model)
        added_names = {c.name for c in diff.added_columns}
        assert "id" in added_names
        assert "color" in added_names

    def test_empty_source_produces_no_entry(self):
        from pgreviewer.cli.commands.diff import _collect_model_diffs

        model_diff_results: list[dict] = []
        _collect_model_diffs("app/empty.py", "", "main", model_diff_results)
        assert model_diff_results == []
