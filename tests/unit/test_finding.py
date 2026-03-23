"""Tests for the Finding contract (Finding, Category, FindingSet)."""

from __future__ import annotations

from pgreviewer.core.finding import Category, Finding, FindingSet
from pgreviewer.core.models import Severity

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make(
    detector: str = "TestDetector",
    severity: Severity = Severity.WARNING,
    category: Category = Category.MIGRATION_SAFETY,
    title: str = "Test finding",
    evidence: str = "SELECT 1",
    suggestion: str = "Fix it",
    table: str | None = "users",
    file_path: str | None = "migrations/001.py",
    line_number: int | None = 10,
    **kwargs,
) -> Finding:
    return Finding(
        detector=detector,
        severity=severity,
        category=category,
        title=title,
        evidence=evidence,
        suggestion=suggestion,
        table=table,
        file_path=file_path,
        line_number=line_number,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Category enum
# ---------------------------------------------------------------------------


def test_category_values() -> None:
    assert Category.MIGRATION_SAFETY == "migration_safety"
    assert Category.QUERY_PERFORMANCE == "query_performance"
    assert Category.CODE_PATTERN == "code_pattern"


# ---------------------------------------------------------------------------
# Finding dataclass
# ---------------------------------------------------------------------------


def test_finding_required_fields() -> None:
    f = Finding(
        detector="FKWithoutIndexDetector",
        severity=Severity.WARNING,
        category=Category.MIGRATION_SAFETY,
        title="Missing FK index",
        evidence=(
            "ALTER TABLE orders ADD CONSTRAINT fk_user"
            " FOREIGN KEY (user_id) REFERENCES users(id);"
        ),
        suggestion="CREATE INDEX CONCURRENTLY ON orders(user_id);",
    )
    assert f.detector == "FKWithoutIndexDetector"
    assert f.severity == Severity.WARNING
    assert f.category == Category.MIGRATION_SAFETY
    assert f.table is None
    assert f.file_path is None
    assert f.line_number is None
    assert f.explanation is None
    assert f.confidence == 1.0
    assert f.metadata == {}


def test_finding_optional_fields() -> None:
    f = _make(
        explanation="LLM says: add an index",
        confidence=0.85,
        metadata={"row_estimate": 50000},
    )
    assert f.explanation == "LLM says: add an index"
    assert f.confidence == 0.85
    assert f.metadata == {"row_estimate": 50000}


def test_finding_dedup_key_components() -> None:
    f = _make(detector="D", table="orders", file_path="m/001.py", line_number=5)
    assert f.dedup_key == ("D", "orders", "m/001.py", 5)


def test_finding_dedup_key_none_fields() -> None:
    f = Finding(
        detector="D",
        severity=Severity.INFO,
        category=Category.QUERY_PERFORMANCE,
        title="t",
        evidence="e",
        suggestion="s",
    )
    assert f.dedup_key == ("D", None, None, None)


# ---------------------------------------------------------------------------
# FindingSet — add and dedup
# ---------------------------------------------------------------------------


def test_findingset_add_new() -> None:
    fs = FindingSet()
    f = _make()
    fs.add(f)
    assert len(fs) == 1
    assert f in fs


def test_findingset_dedup_same_key_keeps_existing_when_same_severity() -> None:
    f1 = _make(title="first")
    f2 = _make(title="second")  # same dedup key, same severity
    fs = FindingSet([f1, f2])
    assert len(fs) == 1
    assert fs.to_list()[0].title == "first"


def test_findingset_dedup_upgrades_to_higher_severity() -> None:
    f_warn = _make(severity=Severity.WARNING, title="warning")
    f_crit = _make(severity=Severity.CRITICAL, title="critical")
    fs = FindingSet([f_warn, f_crit])
    assert len(fs) == 1
    assert fs.to_list()[0].severity == Severity.CRITICAL


def test_findingset_dedup_keeps_existing_when_lower_severity() -> None:
    f_crit = _make(severity=Severity.CRITICAL, title="critical")
    f_info = _make(severity=Severity.INFO, title="info")
    fs = FindingSet([f_crit, f_info])
    assert len(fs) == 1
    assert fs.to_list()[0].severity == Severity.CRITICAL


def test_findingset_different_keys_are_separate() -> None:
    f1 = _make(table="orders")
    f2 = _make(table="users")
    fs = FindingSet([f1, f2])
    assert len(fs) == 2


def test_findingset_from_list() -> None:
    findings = [_make(table="t1"), _make(table="t2"), _make(table="t3")]
    fs = FindingSet(findings)
    assert len(fs) == 3


# ---------------------------------------------------------------------------
# FindingSet — merge
# ---------------------------------------------------------------------------


def test_findingset_merge_disjoint() -> None:
    fs1 = FindingSet([_make(table="orders")])
    fs2 = FindingSet([_make(table="users")])
    merged = fs1.merge(fs2)
    assert len(merged) == 2


def test_findingset_merge_overlap_keeps_higher_severity() -> None:
    f_warn = _make(severity=Severity.WARNING)
    f_crit = _make(severity=Severity.CRITICAL)
    fs1 = FindingSet([f_warn])
    fs2 = FindingSet([f_crit])
    merged = fs1.merge(fs2)
    assert len(merged) == 1
    assert merged.to_list()[0].severity == Severity.CRITICAL


def test_findingset_merge_does_not_mutate_originals() -> None:
    fs1 = FindingSet([_make(table="orders", severity=Severity.WARNING)])
    fs2 = FindingSet([_make(table="orders", severity=Severity.CRITICAL)])
    _ = fs1.merge(fs2)
    assert fs1.to_list()[0].severity == Severity.WARNING  # unchanged


# ---------------------------------------------------------------------------
# FindingSet — filtering
# ---------------------------------------------------------------------------


def test_filter_by_severity() -> None:
    fs = FindingSet(
        [
            _make(table="a", severity=Severity.CRITICAL),
            _make(table="b", severity=Severity.WARNING),
            _make(table="c", severity=Severity.INFO),
        ]
    )
    criticals = fs.filter_by_severity(Severity.CRITICAL)
    assert len(criticals) == 1
    assert criticals.to_list()[0].table == "a"


def test_filter_by_severity_multiple() -> None:
    fs = FindingSet(
        [
            _make(table="a", severity=Severity.CRITICAL),
            _make(table="b", severity=Severity.WARNING),
            _make(table="c", severity=Severity.INFO),
        ]
    )
    result = fs.filter_by_severity(Severity.CRITICAL, Severity.WARNING)
    assert len(result) == 2


def test_filter_by_category() -> None:
    fs = FindingSet(
        [
            _make(table="a", category=Category.MIGRATION_SAFETY),
            _make(table="b", category=Category.QUERY_PERFORMANCE),
            _make(table="c", category=Category.CODE_PATTERN),
        ]
    )
    result = fs.filter_by_category(Category.QUERY_PERFORMANCE)
    assert len(result) == 1
    assert result.to_list()[0].table == "b"


def test_filter_by_detector() -> None:
    fs = FindingSet(
        [
            _make(table="a", detector="DetectorA"),
            _make(table="b", detector="DetectorB"),
        ]
    )
    result = fs.filter_by_detector("DetectorA")
    assert len(result) == 1
    assert result.to_list()[0].detector == "DetectorA"


def test_filter_by_table() -> None:
    fs = FindingSet(
        [
            _make(table="orders"),
            _make(table="users"),
            _make(table="products"),
        ]
    )
    result = fs.filter_by_table("orders", "users")
    assert len(result) == 2


# ---------------------------------------------------------------------------
# FindingSet — collection protocol
# ---------------------------------------------------------------------------


def test_findingset_iter() -> None:
    findings = [_make(table="t1"), _make(table="t2")]
    fs = FindingSet(findings)
    assert list(fs) == [findings[0], findings[1]]


def test_findingset_contains_true() -> None:
    f = _make()
    fs = FindingSet([f])
    assert f in fs


def test_findingset_contains_false() -> None:
    f = _make()
    fs = FindingSet()
    assert f not in fs


def test_findingset_contains_non_finding() -> None:
    fs = FindingSet([_make()])
    assert "not a finding" not in fs


def test_findingset_repr() -> None:
    fs = FindingSet([_make(), _make(table="other")])
    assert "2 findings" in repr(fs)


def test_findingset_to_list_is_snapshot() -> None:
    f = _make()
    fs = FindingSet([f])
    snapshot = fs.to_list()
    fs.add(_make(table="other"))
    assert len(snapshot) == 1  # snapshot not affected by later add


# ---------------------------------------------------------------------------
# Acceptance criteria: all Issue fields map to Finding
# ---------------------------------------------------------------------------


def test_issue_fields_representable_as_finding() -> None:
    """All fields from the existing Issue dataclass can be represented in Finding."""
    # Issue has: severity, detector_name, description, affected_table,
    #   affected_columns, suggested_action, confidence, context,
    #   fix_type, cause_file, cause_line, cause_context
    f = Finding(
        detector="FKWithoutIndexDetector",
        severity=Severity.WARNING,
        category=Category.MIGRATION_SAFETY,
        title="Missing FK index on orders.user_id",  # ← description
        evidence="ALTER TABLE orders ADD CONSTRAINT …",  # ← cause_context
        suggestion="CREATE INDEX CONCURRENTLY …",  # ← suggested_action
        table="orders",  # ← affected_table
        file_path="migrations/001_add_fk.py",  # ← cause_file
        line_number=42,  # ← cause_line
        confidence=0.9,
        metadata={
            "affected_columns": ["user_id"],  # ← affected_columns
            "fix_type": "replace",  # ← fix_type
            "context": {"row_estimate": 50000},  # ← context
        },
    )
    assert f.detector == "FKWithoutIndexDetector"
    assert f.metadata["affected_columns"] == ["user_id"]
