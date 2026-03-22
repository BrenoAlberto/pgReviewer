"""
False-positive tests for QueryInLoopDetector.

Each test documents a *legitimate* pattern that should NOT be flagged as N+1
(or should be flagged at INFO, not CRITICAL), along with the reasoning for why
it is safe.  A failing test here means the detector is generating a noisy or
wrong result that would erode developer trust.
"""

import pytest

from pgreviewer.analysis.code_pattern_detectors.base import QueryCatalog
from pgreviewer.analysis.code_pattern_detectors.python.query_in_loop import QueryInLoopDetector
from pgreviewer.core.models import Severity

from .conftest import parse_python_source

detector = QueryInLoopDetector()


# ── 1. fetchone() consuming an already-executed cursor ────────────────────────


def test_fetchone_cursor_iteration_is_not_flagged() -> None:
    """
    fetchone() inside a while-loop is the idiomatic pattern for iterating over
    a cursor that was opened with a single execute() call.  The loop issues
    exactly ONE query round-trip (the execute), not N.  Flagging this as N+1
    is wrong.

    Expected: no issues (or at most INFO — the cursor iteration produces rows,
    not new queries).
    """
    source = (
        "def stream_events(conn, cutoff):\n"
        "    conn.execute('SELECT * FROM events WHERE ts > %s', [cutoff])\n"
        "    while True:\n"
        "        row = conn.fetchone()\n"
        "        if row is None:\n"
        "            break\n"
        "        process(row)\n"
    )
    parsed = parse_python_source("app/services/event_stream.py", source)
    issues = detector.detect([parsed], QueryCatalog())
    assert issues == [], (
        "fetchone() consuming an already-opened cursor must not be flagged as N+1"
    )


# ── 2. Query in for-loop iterable expression ─────────────────────────────────


def test_query_as_for_loop_iterable_is_not_flagged() -> None:
    """
    `for task in db.query(Task).filter(...).all():` — the db.query() call is
    the iterable expression of the for-statement.  It executes exactly once
    before any iteration, so it is NOT a per-row query and must not be flagged.

    This is the correct batched pattern — semantically identical to:
        tasks = db.query(Task).filter(...).all()
        for task in tasks: ...
    """
    source = (
        "def get_standup_report(db):\n"
        "    projects = db.query(Project).all()\n"
        "    project_ids = [p.id for p in projects]\n"
        "    tasks_by_project = {p.id: [] for p in projects}\n"
        "    for task in (\n"
        "        db.query(Task)\n"
        "        .filter(Task.project_id.in_(project_ids))\n"
        "        .all()\n"
        "    ):\n"
        "        tasks_by_project[task.project_id].append(task)\n"
    )
    parsed = parse_python_source("app/routers/standup.py", source)
    issues = detector.detect([parsed], QueryCatalog())
    assert issues == [], (
        "db.query() in the for-loop iterable executes once — must not be flagged"
    )


# ── 3. Chunked / paginated batch processing ───────────────────────────────────


@pytest.mark.xfail(
    reason=(
        "Known limitation: keyset pagination looks identical to N+1 at the AST level. "
        "The detector cannot statically distinguish 'fetch next page' from "
        "'fetch per-row dependent record' without query-text heuristics that "
        "would introduce their own false negatives. "
        "Use # pgreviewer:ignore[query_in_loop] for intentional paginated scans."
    ),
    strict=True,
)
def test_keyset_pagination_loop_is_not_flagged() -> None:
    """
    Keyset pagination deliberately queries in a loop — each iteration fetches
    the NEXT page, not a per-row dependent query.  The loop variable (last_id)
    comes from the batch result, but the query is fetching a new independent
    page, not a dependent child record.

    Expected: no issues.  This is a standard bulk-processing pattern used in
    ETL, data exports, and backfills.
    """
    source = (
        "def export_all_events(conn):\n"
        "    last_id = 0\n"
        "    while True:\n"
        "        rows = conn.fetchall(\n"
        "            'SELECT * FROM events WHERE id > %s LIMIT 1000',\n"
        "            [last_id],\n"
        "        )\n"
        "        if not rows:\n"
        "            break\n"
        "        export(rows)\n"
        "        last_id = rows[-1]['id']\n"
    )
    parsed = parse_python_source("app/jobs/export.py", source)
    issues = detector.detect([parsed], QueryCatalog())
    assert issues == [], (
        "keyset-pagination pattern (intentional batched scan) must not be flagged"
    )


# ── 3. Retry loop ─────────────────────────────────────────────────────────────


def test_small_retry_loop_is_at_most_info() -> None:
    """
    A retry loop with range(3) is NOT an N+1 — it issues at most 3 queries for
    the same operation, not one per data row.  The detector should not raise
    CRITICAL for this; INFO is acceptable if anything.
    """
    source = (
        "def fetch_with_retry(conn):\n"
        "    for attempt in range(3):\n"
        "        try:\n"
        "            return conn.fetchone('SELECT 1')\n"
        "        except Exception:\n"
        "            continue\n"
    )
    parsed = parse_python_source("app/utils/retry.py", source)
    issues = detector.detect([parsed], QueryCatalog())
    for issue in issues:
        assert issue.severity == Severity.INFO, (
            f"retry loop with range(3) must not be CRITICAL, got {issue.severity}"
        )


# ── 4. Loop over a known-constant list (not data-driven) ─────────────────────


@pytest.mark.xfail(
    reason=(
        "Known limitation: _is_small_iterable resolves inline literals and range() "
        "only. A named constant (REGIONS = [...]) is an identifier node — resolving "
        "it would require intra-file variable tracking. For now, use an inline "
        "list literal or # pgreviewer:ignore[query_in_loop] for constant iterables "
        "with N >= 10."
    ),
    strict=True,
)
def test_loop_over_constant_config_list_is_not_critical() -> None:
    """
    Looping over a small hard-coded list of config values is NOT N+1 — N is
    fixed at coding time, not proportional to data volume.  Raising CRITICAL
    here is misleading.

    Note: the current _is_small_iterable only catches inline list/tuple/range
    literals.  A constant defined as a module-level variable is not detected as
    small.  This test documents the known limitation without asserting a fix.
    """
    source = (
        "REGIONS = ['us-east', 'us-west', 'eu-west']\n"
        "\n"
        "def load_region_config(conn):\n"
        "    for region in REGIONS:\n"
        "        row = conn.fetchone(\n"
        "            'SELECT * FROM config WHERE region = %s', [region]\n"
        "        )\n"
        "        process(row)\n"
    )
    parsed = parse_python_source("app/config_loader.py", source)
    issues = detector.detect([parsed], QueryCatalog())
    # If the detector fires, it must be at most INFO — not CRITICAL.
    # A named constant iterable (REGIONS) is not data-driven N+1.
    for issue in issues:
        assert issue.severity != Severity.CRITICAL, (
            "loop over a named constant list must not be CRITICAL"
        )


# ── 5. Inline ignore suppresses the finding ──────────────────────────────────


def test_inline_ignore_comment_suppresses_detection() -> None:
    """
    # pgreviewer:ignore[query_in_loop] on the loop line must fully suppress
    the finding.  This is the escape hatch for cases the detector can't
    distinguish statically — developers know their code.
    """
    source = (
        "def process_items(conn, item_ids):\n"
        "    for item_id in item_ids:  # pgreviewer:ignore[query_in_loop]\n"
        "        row = conn.fetchone('SELECT * FROM items WHERE id = %s', [item_id])\n"
        "        send_to_legacy_system(row)  # cannot batch: legacy API is per-item\n"
    )
    parsed = parse_python_source("app/legacy_sync.py", source)
    issues = detector.detect([parsed], QueryCatalog())
    assert issues == [], (
        "pgreviewer:ignore[query_in_loop] on the loop line must suppress the issue"
    )


# ── 6. Path ignore pattern suppresses detection ───────────────────────────────


def test_path_ignore_pattern_suppresses_detection(monkeypatch) -> None:
    """
    Files matching QUERY_IN_LOOP_IGNORE_PATTERNS must be fully suppressed.
    Management commands, migration scripts, and one-off data repair jobs are
    legitimate contexts where serial query-per-item is intentional.
    """
    from pgreviewer.config import settings

    monkeypatch.setattr(
        settings,
        "QUERY_IN_LOOP_IGNORE_PATTERNS",
        ["*/management/commands/*"],
    )
    source = (
        "def handle(self):\n"
        "    for user_id in old_ids:\n"
        "        self.conn.execute(\n"
        "            'UPDATE users SET migrated=true WHERE id=%s', [user_id]\n"
        "        )\n"
        "        self.conn.commit()\n"
    )
    parsed = parse_python_source("app/management/commands/migrate_users.py", source)
    issues = detector.detect([parsed], QueryCatalog())
    assert issues == [], (
        "files matching QUERY_IN_LOOP_IGNORE_PATTERNS must be fully suppressed"
    )


# ── 7. executemany is not an N+1 ─────────────────────────────────────────────


def test_executemany_in_loop_is_not_flagged(monkeypatch) -> None:
    """
    executemany() issues a single round-trip for all rows regardless of how
    many items are in the list.  It must never be flagged as N+1.
    """
    from pgreviewer.config import settings

    monkeypatch.setattr(
        settings,
        "QUERY_IN_LOOP_FUNCTION_ALLOWLIST",
        ["executemany"],
    )
    source = (
        "def bulk_insert(conn, rows):\n"
        "    for chunk in chunks(rows, 500):\n"
        "        conn.executemany('INSERT INTO log VALUES (%s, %s)', chunk)\n"
    )
    parsed = parse_python_source("app/db/bulk.py", source)
    issues = detector.detect([parsed], QueryCatalog())
    assert issues == [], "executemany must be allowlisted and never flagged as N+1"
