# Demo 03 — SQLAlchemy Declarative Models

This demo shows pgReviewer catching schema issues directly from SQLAlchemy
`DeclarativeBase` model source.

No migration files and no SQL files are required: model-diff analysis is static,
source-only inspection.

---

## Files

| File | Purpose |
|---|---|
| `models.py` | Adds declarative models with an intentionally unindexed FK and a `back_populates` relationship on that FK |
| `.pgreviewer.yml` | Overrides `missing_fk_index` severity to `WARNING` for this demo |

---

## Expected findings

From the **repo root** (using `--config` to load the demo's severity overrides):

```bash
git diff --no-index /dev/null demos/03-sqlalchemy-models/models.py \
  > /tmp/demo03.diff || true

pgr diff /tmp/demo03.diff --config demos/03-sqlalchemy-models/.pgreviewer.yml
```

Or from this demo directory on a branch that adds `models.py`:

```bash
pgr diff --git-ref main
```

| Severity | Detector | Finding |
|---|---|---|
| WARNING | `missing_fk_index` | `ForeignKey` column `events.account_id` has no `index=True` |

`models.py` also includes a `UniqueConstraint` in `__table_args__` (without an
explicit `Index(...)`) so teams can see how model-only schema intent appears in
model diffs.

---

## Why this demo matters

Teams that manage schema via ORM models (instead of hand-written SQL migrations)
can still get early index/performance feedback in CI.

pgReviewer parses model source and compares it to `--git-ref` content, so this
path works without a live database connection.
