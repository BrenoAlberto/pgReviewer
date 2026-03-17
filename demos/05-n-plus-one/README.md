# Demo 05 — N+1 Query Pattern in Python Code

This demo shows pgReviewer catching the classic **N+1 query** pattern in
Flask/FastAPI-style Python code.

No database or migration files are required — this detector is purely static
(AST-based) and analyzes Python source code.

---

## Files

| File | Purpose |
|---|---|
| `views.py` | Simulates a view function that fetches orders and then queries users inside a loop |
| `repository.py` | Shows the same loop-query anti-pattern in a repository class |
| `.pgreviewer.yml` | Enables `query_in_loop` for this demo scope |

---

## Expected findings

From the **repo root**:

```bash
git diff --no-index /dev/null demos/05-n-plus-one/views.py > /tmp/d05_1.diff || true
git diff --no-index /dev/null demos/05-n-plus-one/repository.py > /tmp/d05_2.diff || true
cat /tmp/d05_1.diff /tmp/d05_2.diff > /tmp/demo05.diff

pgr diff /tmp/demo05.diff --config demos/05-n-plus-one/.pgreviewer.yml
```

Or from this demo directory on a branch that adds these files:

```bash
pgr diff --git-ref main
```

| Severity | Detector | File |
|---|---|---|
| CRITICAL | `query_in_loop` | `views.py` — `execute()` inside `for order in orders:` loop |
| CRITICAL | `query_in_loop` | `repository.py` — same pattern in repository method |

`query_in_loop` flags the repeated per-item query inside loops, which is a
strong indicator of an N+1 access pattern.

---

## Why this demo matters

Python-heavy teams can validate code-review detection of N+1 patterns in
application code before hitting production load, without needing a live
database connection.
