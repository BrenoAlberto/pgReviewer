"""Finding contract — the standard output type for all pgReviewer detectors.

Every detector (migration-safety, query-performance, code-pattern) produces
``Finding`` instances.  Downstream consumers (policy engine, comment formatter,
LLM enrichment pipeline) work exclusively against this contract so the
internals of each detector can evolve independently.

Hierarchy (first match wins)
----------------------------
``FindingSet`` → ordered collection of ``Finding`` objects with dedup, merge,
and filter helpers.
``Finding`` → a single actionable observation from a detector.
``Category`` → broad family the finding belongs to (MIGRATION_SAFETY, …).
``Severity`` → already defined in :mod:`pgreviewer.core.models`; re-exported
here so importers only need one module.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator

from pgreviewer.core.models import Severity


class Category(StrEnum):
    """Broad detector family — used by the policy engine and comment formatter."""

    MIGRATION_SAFETY = "migration_safety"
    """DDL changes that may cause downtime, data loss, or lock contention."""

    QUERY_PERFORMANCE = "query_performance"
    """EXPLAIN-based findings: sequential scans, high cost, nested loops."""

    CODE_PATTERN = "code_pattern"
    """Static code patterns: N+1 queries, SQL injection, etc."""


class FixType(StrEnum):
    """How the comment formatter should render the suggestion.

    Used by the GitHub review-suggestions feature to decide between a
    ``suggestion`` block (replace), a plain comment (additive/advisory),
    or no inline annotation at all.
    """

    REPLACE = "replace"
    """The affected line should be swapped — use a ```suggestion block."""

    ADDITIVE = "additive"
    """Fix requires adding new code alongside, not replacing the line."""

    ADVISORY = "advisory"
    """No auto-fix possible; explain the problem only."""


@dataclass
class Finding:
    """A single actionable observation produced by a detector.

    Required fields
    ---------------
    detector : str
        Class name of the detector that raised this finding, e.g.
        ``"FKWithoutIndexDetector"``.
    severity : Severity
        CRITICAL, WARNING, or INFO.
    category : Category
        Broad detector family.
    title : str
        One-line human-readable summary (≤ 80 chars).
    evidence : str
        The SQL snippet, EXPLAIN node, or code fragment that triggered the
        finding.  Used verbatim in comments and LLM prompts.
    suggestion : str
        Copy-ready fix (SQL, code, or advisory text).

    Optional fields
    ---------------
    table : str | None
        Primary table affected, if applicable.
    file_path : str | None
        Path of the diff file that contains the finding.
    line_number : int | None
        Line number within *file_path* (1-based).
    fix_type : FixType
        How the comment formatter should render the suggestion.
        Defaults to ``FixType.REPLACE``.
    explanation : str | None
        LLM-generated prose explanation.  ``None`` until the enrichment
        pipeline runs.
    confidence : float
        Detector certainty in [0.0, 1.0].  Defaults to 1.0 (fully certain).
    cause_file : str | None
        For cross-cutting (Type B) findings: the file where the
        *cause* of this finding originated.  ``None`` for Type A
        findings where cause and effect are at the same location.
    cause_line : int | None
        Line number in *cause_file* (1-based).
    cause_context : str | None
        Human-readable snippet showing the causal change.
    metadata : dict[str, Any]
        Detector-specific payload — cost figures, row estimates, index
        names, etc.  Not part of the stable contract; consumers must
        handle missing keys gracefully.
    """

    # --- required ---
    detector: str
    severity: Severity
    category: Category
    title: str
    evidence: str
    suggestion: str

    # --- optional ---
    table: str | None = None
    file_path: str | None = None
    line_number: int | None = None
    fix_type: FixType = FixType.REPLACE
    explanation: str | None = None
    confidence: float = 1.0
    cause_file: str | None = None
    cause_line: int | None = None
    cause_context: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Identity / dedup key
    # ------------------------------------------------------------------

    @property
    def dedup_key(self) -> tuple[str, str | None, str | None, int | None]:
        """Stable identity for deduplication within a ``FindingSet``.

        Two findings with the same key are considered the same observation.
        The higher-severity one is kept when merging.
        """
        return (self.detector, self.table, self.file_path, self.line_number)


class FindingSet:
    """Ordered collection of :class:`Finding` objects.

    Supports deduplication, merging, and filtering so pipeline stages can
    accumulate findings incrementally without worrying about duplicates.

    Deduplication
    -------------
    When :meth:`add` is called with a finding whose :attr:`~Finding.dedup_key`
    already exists, the **higher-severity** finding is kept (CRITICAL >
    WARNING > INFO).  Ties keep the existing entry.
    """

    _SEVERITY_RANK: dict[Severity, int] = {
        Severity.INFO: 0,
        Severity.WARNING: 1,
        Severity.CRITICAL: 2,
    }

    def __init__(self, findings: list[Finding] | None = None) -> None:
        self._index: dict[tuple, int] = {}  # dedup_key → position in _findings
        self._findings: list[Finding] = []
        for f in findings or []:
            self.add(f)

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def add(self, finding: Finding) -> None:
        """Add *finding*, replacing an existing entry only if the new one has
        strictly higher severity."""
        key = finding.dedup_key
        if key in self._index:
            existing = self._findings[self._index[key]]
            if (
                self._SEVERITY_RANK[finding.severity]
                > self._SEVERITY_RANK[existing.severity]
            ):
                self._findings[self._index[key]] = finding
            # lower-or-equal severity → keep existing, do nothing
        else:
            self._index[key] = len(self._findings)
            self._findings.append(finding)

    def merge(self, other: FindingSet) -> FindingSet:
        """Return a new ``FindingSet`` containing findings from both sets.

        Deduplication rules apply: for shared keys the higher severity wins.
        """
        result = FindingSet(list(self._findings))
        for f in other:
            result.add(f)
        return result

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    def filter_by_severity(self, *severities: Severity) -> FindingSet:
        """Return a new set containing only findings with the given severities."""
        wanted = set(severities)
        return FindingSet([f for f in self._findings if f.severity in wanted])

    def filter_by_category(self, *categories: Category) -> FindingSet:
        """Return a new set containing only findings in the given categories."""
        wanted = set(categories)
        return FindingSet([f for f in self._findings if f.category in wanted])

    def filter_by_detector(self, *detectors: str) -> FindingSet:
        """Return a new set containing only findings from the given detectors."""
        wanted = set(detectors)
        return FindingSet([f for f in self._findings if f.detector in wanted])

    def filter_by_table(self, *tables: str) -> FindingSet:
        """Return a new set containing only findings that affect the given tables."""
        wanted = set(tables)
        return FindingSet([f for f in self._findings if f.table in wanted])

    # ------------------------------------------------------------------
    # Collection protocol
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._findings)

    def __iter__(self) -> Iterator[Finding]:
        return iter(self._findings)

    def __contains__(self, finding: object) -> bool:
        if not isinstance(finding, Finding):
            return False
        return finding.dedup_key in self._index

    def __repr__(self) -> str:
        return f"FindingSet({len(self._findings)} findings)"

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def to_list(self) -> list[Finding]:
        """Return a snapshot of all findings in insertion order."""
        return list(self._findings)
